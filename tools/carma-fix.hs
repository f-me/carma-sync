{-# LANGUAGE OverloadedStrings #-}

module Main (
    main
    ) where

import Prelude hiding (log)

import Control.Arrow
import qualified Control.Exception as E
import Control.DeepSeq
import Control.Monad
import Control.Monad.Reader
import Control.Monad.Error
import Data.String
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.ByteString.Char8 as C8
import qualified Carma.ModelTables as MT
import qualified Database.Redis as R
import System.Environment
import System.Log.Simple

instance Error R.Reply where
    strMsg = R.Error . fromString
    noMsg = strMsg noMsg

main :: IO ()
main = do
    as <- getArgs
    case as of
        ["--help"] -> putStrLn "Usage: carma-fix [model]"
        [m] -> fixVals (Just m)
        [] -> fixVals Nothing
        _ -> putStrLn "Invalid arguments"

fixVals :: Maybe String -> IO ()
fixVals m = do
    descs <- MT.loadTables
        "resources/site-config/models"
        "resources/site-config/field-groups.json"
    let
        fields = map (MT.tableModel &&& (("id":) . map (fromString . MT.columnName) . MT.tableFields)) descs

    l <- newLog (constant []) [logger text (file "log/carma-fix.log")]

    conn <- R.connect R.defaultConnectInfo
    case m of
        Nothing -> mapM_ (withLog l . runFix conn . fst) fields
        Just m' -> maybe
            (error $ "Invalid model " ++ m')
            (const $ withLog l (runFix conn m'))
            (lookup m' fields)

runFix :: R.Connection -> String -> ReaderT Log IO ()
runFix con m = scope "fix" $ scope (fromString m) $ void $ runErrorT $ do
    items <- inRedis $ R.keys $ fromString $ m ++ ":*"
    let
        cnt = length items
    lift $ forM_ (zip [1..] items) $ \(i, it) -> runErrorT $ do
        liftIO $ putStrLn $ m ++ ":" ++ show i ++ "/" ++ show cnt
        itName <- decodeName it $ do
            lift $ logInvalid "Invalid key: " it
            inRedis $ R.del [it]
        lift $ scope itName $ void $ runErrorT $ do
            vals <- inRedis $ R.hgetall it
            forM_ vals $ \(f, v) -> do
                fName <- decodeName f $ do
                    lift $ logInvalid "Invalid field name: " f
                    inRedis $ R.hdel it [f]
                decodeName v $ do
                    lift $ logInvalid (T.concat ["Invalid field ", fName, " : "]) v
                    inRedis $ R.hset it f ""
    where
        inRedis :: MonadIO m => R.Redis (Either R.Reply a) -> ErrorT R.Reply m a
        inRedis = ErrorT . liftIO . R.runRedis con

decodeName :: MonadIO m => C8.ByteString -> ErrorT R.Reply m a -> ErrorT R.Reply m T.Text
decodeName bs onErr = liftIO (tryDecode bs) >>= maybe (onErr >> throwError noMsg) return

tryDecode :: C8.ByteString -> IO (Maybe T.Text)
tryDecode bs = E.catch convert onError where
    convert = fmap Just $ E.evaluate (force $ T.decodeUtf8 bs)
    onError :: E.SomeException -> IO (Maybe T.Text)
    onError _ = return Nothing

logInvalid :: MonadLog m => T.Text -> C8.ByteString -> m ()
logInvalid txt v = log Warning $ T.concat [txt, fromString $ show v]

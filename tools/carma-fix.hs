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
import Control.Monad.CatchIO
import Control.Monad.IO.Class
import Data.List
import Data.String
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.ByteString.Char8 as C8
import qualified Carma.ModelTables as MT
import qualified Database.Redis as R
import System.Environment
import System.Log

instance Error R.Reply where
    strMsg = R.Error . fromString
    noMsg = strMsg noMsg

main :: IO ()
main = do
    as <- getArgs
    case as of
        ["--help"] -> do
            putStrLn "Usage: carma-fix [model]"
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
    items <- ErrorT $ liftIO $ R.runRedis con $ R.keys $ fromString $ m ++ ":*"
    let
        cnt = length items
    lift $ forM_ (zip [1..] items) $ \(i, it) -> scope (T.decodeUtf8 it) $ runErrorT $ do
        liftIO $ putStrLn $ m ++ ":" ++ show i ++ "/" ++ show cnt
        vals <- ErrorT $ liftIO $ R.runRedis con $ R.hgetall it
        forM_ vals $ \(f, v) -> catch
            (void $ liftIO $ E.evaluate (force $ T.decodeUtf8 v))
            (lift . onError it f v)
        ErrorT $ liftIO $ R.runRedis con $ R.hmset it vals
    where
        onError :: MonadLog m => C8.ByteString -> C8.ByteString -> C8.ByteString -> E.SomeException -> m ()
        onError k f v _ = log Warning $ T.concat ["hget ", T.decodeUtf8 k, " ", T.decodeUtf8 f, " = ", fromString $ show v]

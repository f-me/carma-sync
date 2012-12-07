{-# LANGUAGE OverloadedStrings #-}

module Main (
    main
    ) where

import Control.Arrow
import Control.Monad
import Control.Monad.Error
import Control.Monad.IO.Class
import Data.List
import Data.String
import qualified Data.ByteString.Char8 as C8
import qualified Carma.ModelTables as MT
import qualified Database.Redis as R
import System.Environment

instance Error R.Reply where
    strMsg = R.Error . fromString
    noMsg = strMsg noMsg

main :: IO ()
main = do
    as <- getArgs
    case as of
        ["--help"] -> do
            putStrLn "Usage: carma-clean [model]"
        [m] -> clean (Just m)
        [] -> clean Nothing
        _ -> putStrLn "Invalid arguments"

clean :: Maybe String -> IO ()
clean m = do
    descs <- MT.loadTables
        "resources/site-config/models"
        "resources/site-config/field-groups.json"
    let
        fields = map (MT.tableModel &&& (("id":) . map (fromString . MT.columnName) . MT.tableFields)) descs

    conn <- R.connect R.defaultConnectInfo
    R.runRedis conn $ do
        case m of
            Nothing -> mapM_ (uncurry runClean) fields
            Just m' -> maybe
                (error $ "Invalid model " ++ m')
                (runClean m')
                (lookup m' fields)

runClean :: String -> [C8.ByteString] -> R.Redis ()
runClean m fs = void $ runErrorT $ do
    items <- ErrorT $ R.keys $ fromString $ m ++ ":*"
    let
        cnt = length items
    lift $ forM_ (zip [1..] items) $ \(i, it) -> runErrorT $ do
        liftIO $ putStrLn $ m ++ ":" ++ show i ++ "/" ++ show cnt
        ks <- ErrorT $ liftM (fmap (\\ fs)) $ R.hkeys it
        -- Redis 2.2 can't delete multiple fields
        mapM_ (\k -> ErrorT (R.hdel it [k])) ks

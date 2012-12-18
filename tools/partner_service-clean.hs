{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Applicative
import Control.Monad.Error
import Control.Monad.IO.Class

import qualified Data.Map as M

import Data.Either
import Data.Maybe
import Data.List
import qualified Data.ByteString.Char8 as B

import qualified Database.Redis as R
import qualified Database.PostgreSQL.Simple as P
import Database.PostgreSQL.Simple

pconinfo :: P.ConnectInfo
pconinfo = P.defaultConnectInfo {
    P.connectUser = "carma_db_sync",
    P.connectDatabase = "carma",
    P.connectPassword = "pass" }

main = do
  rcon <- R.connect R.defaultConnectInfo
  pcon <- P.connect pconinfo
  Right pkeys <- R.runRedis rcon $ R.keys "partner:*"
  Right skeys <- R.runRedis rcon $ R.keys "partner_service:*"
  partners <- getModels rcon pkeys
  services <- getModels rcon skeys

  forM_ partners $ \p -> do
         let pid   = fromJust $ M.lookup "id" p
             psrvs = B.split ',' $ fromMaybe "" $ M.lookup "services" p
             -- find all services, where parentId is current service
             srvs  = map (fromJust . M.lookup "id" ) $
                     filter ((Just pid ==) . M.lookup "parentId") services
             useless = srvs \\ psrvs
             pgids = map readId useless
         when (not (null useless)) $ do
                         putStrLn $ "Services: " ++ show psrvs
                         putStrLn $ "Useless: " ++ show useless
                         putStrLn "="
                         R.runRedis rcon $ R.del useless
                         P.execute pcon
                              "DELETE FROM partner_servicetbl WHERE id in ?" $
                              Only (In pgids)
                         return ()

readId rawid = fst $ fromJust $ B.readInt $ (B.split ':' rawid) !! 1

getModels rcon keys = R.runRedis rcon $ mapM get keys
    where
      get key = do
        Right o <- R.hgetall key
        return $ M.insert "id" key $ M.fromList o

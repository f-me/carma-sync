{-# LANGUAGE ScopedTypeVariables #-}

module Carma.ModelTables (
    -- * Types
    ModelDesc(..), ModelField(..),
    ModelGroups(..),
    TableDesc(..), TableColumn(..),

    -- * Loading
    loadTables,

    -- * Queries
    insertUpdate, insert, update,

    -- * Util
    tableByModel,
    addType,
    typize,
    tableFlatFields
    ) where

import Control.Arrow
import Control.Applicative
import Control.Monad
import Control.Monad.IO.Class
import Data.Maybe
import Data.List hiding (insert)
import Data.Time.Clock.POSIX
import Data.String

import Data.Char
import qualified Data.Vector as V
import qualified Blaze.ByteString.Builder.Char8 as BZ (fromString)
import qualified Data.ByteString.Char8 as C8
import qualified Data.Map as M
import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.ByteString.Lazy as B
import Data.Aeson
import System.FilePath
import System.Directory

import qualified Database.PostgreSQL.Simple as P
import qualified Database.PostgreSQL.Simple.ToField as P
import qualified Database.PostgreSQL.Simple.Types as P

import qualified Data.Model as CM
import qualified Data.Model.Types as CM
import qualified Carma.Model as CM

data ModelDesc = ModelDesc {
    modelName :: String,
    modelFields :: [ModelField] }
        deriving (Eq, Ord, Read, Show)

data ModelField = ModelField {
    fieldName :: String,
    fieldExportable :: Bool,
    -- ^ True iff field must be synchronized to SQL storage (matches
    -- the inverse of @nosql@ meta annotation value).
    fieldSqlType :: Maybe String,
    fieldType :: Maybe String,
    fieldGroup :: Maybe String }
        deriving (Eq, Ord, Read, Show)

instance FromJSON ModelField where
    parseJSON (Object v) = do
      -- Read meta bag
      meta <- v .:? "meta" >>= \case
              Nothing -> pure Nothing
              Just mc ->
                  case mc of
                    (Object mo) -> pure $ Just mo
                    _           -> pure Nothing
      -- Read meta bag contents: nosql boolean flag and sqltype value
      (ignore, sqltype) <-
          case meta of
            Just mv -> (,) <$> (mv .:? "nosql" .!= False) <*> (mv .:? "sqltype")
            Nothing -> pure (False, Nothing)
      ModelField <$>
        v .: "name" <*>
        pure (not ignore) <*>
        pure sqltype <*>
        v .:? "type" <*>
        v .:? "groupName"
    parseJSON _ = empty

instance FromJSON ModelDesc where
    parseJSON (Object v) = ModelDesc <$>
        v .: "name" <*>
        v .: "fields"
    parseJSON _ = empty

data ModelGroups = ModelGroups {
    modelGroups :: M.Map String [ModelField] }
        deriving (Show)

instance FromJSON ModelGroups where
    parseJSON (Object v) = (ModelGroups . M.fromList) <$> (mapM parseField $ HM.toList v) where
        parseField (nm, val) = do
            flds <- parseJSON val
            return (T.unpack nm, flds)
    parseJSON _ = empty

data TableDesc = TableDesc {
    tableName :: String,
    tableModel :: String,
    tableParents :: [TableDesc],
    tableFields :: [TableColumn] }
        deriving (Eq, Ord, Read, Show)

data TableColumn = TableColumn {
    columnName :: String,
    columnType :: String }
        deriving (Eq, Ord, Read, Show)


-- | Generate carma-sync table definition from carma-models data model.
mkTableDesc
    :: forall m . CM.Model m => m -> TableDesc
mkTableDesc _
  = let mi = CM.modelInfo :: CM.ModelInfo m
    in TableDesc
      {tableName
          = T.unpack $ CM.tableName mi
      ,tableModel
          = T.unpack $ fromMaybe (error "BUG: no legacy name")
          $ CM.legacyModelName mi
      ,tableParents
          = maybeToList
          $ (`CM.dispatch` mkTableDesc) =<< CM.parentName mi
      ,tableFields
          = [TableColumn
              (T.unpack $ CM.fd_name fd)
              (T.unpack $ CM.pgTypeName $ CM.fd_pgType fd)
            | fd <- CM.modelOnlyFields mi
            -- drop `id` column in inherited tables
            , maybe True (const $ CM.fd_name fd /= "id") $ CM.parentName mi
            ]
      }

newTables :: [TableDesc]
newTables = catMaybes
    [CM.dispatch m mkTableDesc
    |m <- M.elems CM.legacyModelNames
    ]

-- | Use parent table name to check for service-related tables
isService :: TableDesc -> Bool
isService (TableDesc{tableParents=[TableDesc{tableModel="service"}]}) = True
isService _ = False


-- | Load all models, and generate table descriptions
loadTables :: String -> IO [TableDesc]
loadTables base = do
    ms <- fmap (filter $ isSuffixOf ".js") $ getDirectoryContents base
    ms' <- liftM (map addId) $ mapM (loadTableDesc base) ms
    return $ ms' ++ newTables

--
-- | Insert or update data into table
insertUpdate
    :: MonadIO m
    => P.Connection -> TableDesc -> C8.ByteString -> M.Map C8.ByteString C8.ByteString -> m ()
insertUpdate con tbl i dat = do
    [P.Only b] <- liftIO $ P.query con
        (fromString $ "select count(*) > 0 from " ++ tableName tbl ++ " where id = ?") (P.Only i)
    if b
        then update con tbl i dat
        else insert con tbl (M.insert "id" i dat)

update
    :: MonadIO m
    => P.Connection -> TableDesc -> C8.ByteString -> M.Map C8.ByteString C8.ByteString -> m ()
update con tbl i dat = do
    let (actualNames, actualDats) = removeNonColumns tbl dat
    let setters = map (++ " = ?") actualNames
    _ <- liftIO $ P.execute con
      (fromString
        $ "update " ++ tableName tbl
        ++ " set " ++ intercalate ", " setters
        ++ " where id = ?")
      (actualDats P.:. (P.Only i))
    return ()

insert
    :: MonadIO m
    => P.Connection -> TableDesc -> M.Map C8.ByteString C8.ByteString -> m ()
insert con tbl dat = do
    let (actualNames, actualDats) = removeNonColumns tbl dat
    _ <- liftIO $ P.execute con
        (fromString
          $ "insert into " ++ tableName tbl
          ++ " (" ++ intercalate ", " actualNames ++ ") values ("
          ++ intercalate ", " (replicate (length actualDats) "?")
          ++ ")")
        actualDats
    return ()

-- | Remove invalid fields
removeNonColumns :: TableDesc -> M.Map C8.ByteString C8.ByteString -> ([String], [P.Action])
removeNonColumns tbl = unzip . filter ((`elem` fields) . fst) . map (first C8.unpack) . M.toList . typize tbl . addType tbl where
    fields = map columnName $ tableFlatFields tbl

-- | Load model description and convert to table
loadTableDesc :: String -> String -> IO TableDesc
loadTableDesc base f = do
    d <- loadDesc base f
    either error return $ retype d

-- | Load model description
loadDesc :: String -> String -> IO ModelDesc
loadDesc base f = do
    cts <- B.readFile (base </> f)
    maybe (error "Can't load") return $ decode cts

-- | Convert model description to table description with silly type converting.
retype :: ModelDesc -> Either String TableDesc
retype (ModelDesc nm fs) = TableDesc (nm ++ "tbl") nm [] <$> (nub <$> (mapM retype' $ filter fieldExportable fs)) where
    retype' (ModelField fname _ (Just sqltype) _ _) = return $ TableColumn fname sqltype
    retype' (ModelField fname _ Nothing Nothing _) = return $ TableColumn fname "text"
    retype' (ModelField fname _ Nothing (Just ftype) _) = TableColumn fname <$> maybe unknown Right (lookup ftype retypes) where
        unknown = Right "text"
    retypes = [
        ("datetime", "timestamp"),
        ("dictionary", "text"),
        ("phone", "text"),
        ("checkbox", "bool"),
        ("coords", "geometry(point,4326)"),
        ("date", "timestamp"),
        ("picker", "text"),
        ("map", "text"),
        ("statictext", "text"),
        ("textarea", "text"),
        ("reference", "text"),
        ("file", "text"),
        ("json", "json"),
        ("partnerTable", "text"),
        ("statictext", "text"),
        ("dictionary-many", "text[]" )]

-- | Add id column
addId :: TableDesc -> TableDesc
addId = addColumn "id" "integer"

-- | Add column
addColumn :: String -> String -> TableDesc -> TableDesc
addColumn nm tp (TableDesc n mdl h fs) = TableDesc n mdl h $ (TableColumn nm tp) : fs


-- | Find table for model
tableByModel :: (Eq s, IsString s) => s -> [TableDesc] -> Maybe TableDesc
tableByModel name = find ((== name) . fromString . tableModel)

-- | WORKAROUND: Explicitly add type value for data in service-derived model
addType :: TableDesc -> M.Map C8.ByteString C8.ByteString -> M.Map C8.ByteString C8.ByteString
addType m dat = if isService m then M.insert "type" (fromString $ tableModel m) dat else dat

str :: C8.ByteString -> String
str = T.unpack . T.decodeUtf8


-- | Convert data accord to its types
typize :: TableDesc -> M.Map C8.ByteString C8.ByteString -> M.Map C8.ByteString P.Action
typize tbl = M.mapWithKey convertData where
    convertData k v = fromMaybe (P.toField v) $ do
        t <- fmap columnType $ find ((== (str k)) . columnName) $ tableFlatFields tbl
        conv <- lookup t convertors
        return $ case conv v of
            Left _err -> P.toField P.Null
            Right x   -> x

    convertors :: [(String, C8.ByteString -> Either String P.Action)]
    convertors = [
        ("text", Right . P.toField),
        ("json", Right . P.toField),
        ("bool", fromB),
        ("integer", fromI),
        ("geometry(point,4326)", fromCoords),
        ("timestamp with time zone", fromPosix),
        ("timestamp", fromPosix),
        ("text[]", toArray)]

    fromB :: C8.ByteString -> Either String P.Action
    fromB "" = Right $ P.toField P.Null
    fromB "1" = Right $ P.toField True
    fromB "0" = Right $ P.toField False
    fromB v = Left $ "Not a boolean: " ++ str v

    fromI :: C8.ByteString -> Either String P.Action
    fromI "" = Right $ P.toField P.Null
    fromI v = P.toField <$> asInt v

    fromPosix :: C8.ByteString -> Either String P.Action
    fromPosix "" = Right $ P.toField P.Null
    fromPosix v
      = P.toField . posixSecondsToUTCTime . fromInteger
      <$> asInt v

    asInt :: C8.ByteString -> Either String Integer
    asInt v = case C8.readInteger v of
        Just (x, "") -> Right x
        _ -> Left $ "Not an integer: " ++ str v

    toArray :: C8.ByteString -> Either String P.Action
    -- FIXME: in case of empty string postgres can't determine
    -- type of {}, and I can't find a way to add type explicitly
    -- so just set NULL
    toArray "" = Right $ P.toField P.Null
    toArray v  = Right $ P.toField $ V.fromList $ C8.split ',' v

tryRead :: Read a => C8.ByteString -> Either String (Maybe a)
tryRead bs
    | C8.null bs = Right Nothing
    | C8.pack "null" == bs = Right Nothing
    | otherwise = case reads (C8.unpack bs) of
        [(v, s)] -> if all isSpace s
                    then Right (Just v)
                    else Left ("Can't read value: " ++ C8.unpack bs)
        _ -> Left ("Can't read value: " ++ C8.unpack bs)

-- | Read "52.32,3.45" into POINT. Resulting Action is unescaped.
fromCoords :: C8.ByteString -> Either String P.Action
fromCoords bs =
    case tryRead $ C8.concat ["(", bs, ")"] of
      Right (Just (lon :: Double, lat :: Double)) ->
          Right $ (P.Plain . BZ.fromString) $
                concat ["ST_PointFromText('POINT(",
                        show lon, " ", show lat,
                        ")', 4326)"]
      Left s -> Left s
      Right Nothing -> Right $ P.toField P.Null

-- | Gets fields of table and its parents
tableFlatFields :: TableDesc -> [TableColumn]
tableFlatFields t = concatMap tableFlatFields (tableParents t) ++ tableFields t

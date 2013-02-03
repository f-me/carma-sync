{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}

module Carma.ModelTables (
    -- * Types
    ModelDesc(..), ModelField(..),
    ModelGroups(..),
    TableDesc(..), TableColumn(..),

    -- * Loading
    loadTables,

    -- * Queries
    createExtend,
    insertUpdate, insert, update,

    -- * Util
    tableByModel,
    addType,
    typize,
    tableFlatFields
    ) where

import Prelude hiding (log)

import Control.Arrow
import Control.Applicative
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.CatchIO
import Data.Maybe (fromMaybe)
import Data.List hiding (insert)
import Data.Text (Text)
import Data.Time
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
import System.Log.Simple

import qualified Database.PostgreSQL.Simple as P
import qualified Database.PostgreSQL.Simple.ToField as P
import qualified Database.PostgreSQL.Simple.Types as P

data ModelDesc = ModelDesc {
    modelName :: String,
    modelFields :: [ModelField] }
        deriving (Eq, Ord, Read, Show)

data ModelField = ModelField {
    fieldName :: String,
    fieldSqlType :: Maybe String,
    fieldType :: Maybe String,
    fieldGroup :: Maybe String }
        deriving (Eq, Ord, Read, Show)

instance FromJSON ModelField where
    parseJSON (Object v) = ModelField <$>
        v .: "name" <*>
        (do
            mo <- v .:? "meta"
            case mo of
                Nothing -> pure Nothing
                Just mo' -> case mo' of
                    (Object mv) -> mv .:? "sqltype"
                    _ -> pure Nothing) <*>
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

-- | Load all models, and generate table descriptions
loadTables :: String -> String -> IO [TableDesc]
loadTables base field_groups = do
    gs <- loadGroups field_groups
    ms <- fmap (filter $ isSuffixOf ".js") $ getDirectoryContents base
    liftM (mergeServices . map addId) $ mapM (loadTableDesc gs base) ms

execute_ :: MonadLog m => P.Connection -> P.Query -> m ()
execute_ con q = do
    bs <- liftIO $ P.formatQuery con q ()
    log Trace $ T.decodeUtf8 bs
    liftIO $ P.execute_ con q
    return ()

execute :: (MonadLog m, P.ToRow q) => P.Connection -> P.Query -> q -> m ()
execute con q args = do
    bs <- liftIO $ P.formatQuery con q args
    log Trace $ T.decodeUtf8 bs
    liftIO $ P.execute con q args
    return ()

query :: (MonadLog m, P.FromRow r, P.ToRow q) => P.Connection -> P.Query -> q -> m [r]
query con q args = do
    bs <- liftIO $ P.formatQuery con q args
    log Trace $ T.decodeUtf8 bs
    liftIO $ P.query con q args

-- | Create or extend table
createExtend :: MonadLog m => P.Connection -> TableDesc -> m ()
createExtend con tbl = scope "createExtend" $ do
    ignoreError $ scope "create" $ do
        execute_ con (fromString $ createTableQuery tbl)
        execute_ con (fromString $ createIndexQuery tbl)
        return ()
    mapM_ exec $ extendTableQueries tbl
    mapM_ exec $ inheritTableQueries tbl
    where
        exec q = ignoreError $ scope "extend" $ do
            execute_ con (fromString q)
            return ()

-- | Insert or update data into table
insertUpdate :: MonadLog m => P.Connection -> TableDesc -> C8.ByteString -> M.Map C8.ByteString C8.ByteString -> m ()
insertUpdate con tbl i dat = scope "insertUpdate" $ do
    [P.Only b] <- liftIO $ P.query con
        (fromString $ "select count(*) > 0 from " ++ tableName tbl ++ " where id = ?") (P.Only i)
    if b
        then update con tbl i dat
        else insert con tbl (M.insert "id" i dat)

update :: MonadLog m => P.Connection -> TableDesc -> C8.ByteString -> M.Map C8.ByteString C8.ByteString -> m ()
update con tbl i dat = scope "update" $ do
    liftIO $ P.execute con
        (fromString $ "update " ++ tableName tbl ++ " set " ++ intercalate ", " setters ++ " where id = ?") (actualDats P.:. (P.Only i))
    return ()
    where
        (actualNames, actualDats) = removeNonColumns tbl dat
        setters = map (++ " = ?") actualNames

insert :: MonadLog m => P.Connection -> TableDesc -> M.Map C8.ByteString C8.ByteString -> m ()
insert con tbl dat = scope "insert" $ do
    liftIO $ P.execute con
        (fromString $ "insert into " ++ tableName tbl ++ " (" ++ intercalate ", " actualNames ++ ") values (" ++ intercalate ", " (replicate (length actualDats) "?") ++ ")") actualDats
    return ()
    where
        (actualNames, actualDats) = removeNonColumns tbl dat

-- | Remove invalid fields
removeNonColumns :: TableDesc -> M.Map C8.ByteString C8.ByteString -> ([String], [P.Action])
removeNonColumns tbl = unzip . filter ((`elem` fields) . fst) . map (first C8.unpack) . M.toList . typize tbl . addType tbl where
    fields = map columnName $ tableFlatFields tbl

-- | Load model description and convert to table
loadTableDesc :: ModelGroups -> String -> String -> IO TableDesc
loadTableDesc g base f = do
    d <- loadDesc base f
    ungrouped <- either error return $ ungroup g d
    either error return $ retype ungrouped

-- | Create query for table
createTableQuery :: TableDesc -> String
createTableQuery (TableDesc nm _ inhs flds) = concat $ creating ++ inherits where
    creating = [
        "create table ", nm,
        " (", intercalate ", " (map (\(TableColumn n t) -> n ++ " " ++ t) flds), ")"]
    inherits = if null inhs then [] else [" inherits (", intercalate ", " (map tableName inhs), ")"]

-- | Make index on id
createIndexQuery :: TableDesc -> String
createIndexQuery (TableDesc nm _ _ _) = "create index on " ++ nm ++ " (id)"

-- | Alter table inherit
inheritTableQueries :: TableDesc -> [String]
inheritTableQueries (TableDesc nm _ inhs _) = map inheritTable inhs where
    inheritTable :: TableDesc -> String
    inheritTable (TableDesc p _ _ _) = concat ["alter table ", nm, " inherit ", p]

-- | Alter table add column queries for each field of table
extendTableQueries :: TableDesc -> [String]
extendTableQueries (TableDesc nm _ inhs flds) = map extendColumn flds ++ concatMap (map extendColumn . tableFields) inhs where
    extendColumn :: TableColumn -> String
    extendColumn (TableColumn n t) = concat ["alter table ", nm, " add column ", n, " ", t]

-- | Load model description
loadDesc :: String -> String -> IO ModelDesc
loadDesc base f = do
    cts <- B.readFile (base </> f)
    maybe (error "Can't load") return $ decode cts

-- | Load group fields
loadGroups :: String -> IO ModelGroups
loadGroups field_groups = do
    cts <- B.readFile field_groups
    maybe (error "Can't load") return $ decode cts

-- | Unfold groups, adding fields from groups to model (with underscored prefix)
ungroup :: ModelGroups -> ModelDesc -> Either String ModelDesc
ungroup (ModelGroups g) (ModelDesc nm fs) = (ModelDesc nm . concat) <$> mapM ungroup' fs where
    ungroup' (ModelField fname sqltype ftype Nothing) = return [ModelField fname sqltype ftype Nothing]
    ungroup' (ModelField fname sqltype ftype (Just gname)) =
        maybe
            (Left $ "Can't find group " ++ gname)
            (return . map appends)
            (M.lookup gname g)
        where
            appends (ModelField fname' sqltype' ftype' _) = ModelField (fname ++ "_" ++ fname') (sqltype' `mplus` sqltype) (ftype' `mplus` ftype) Nothing

-- | Convert model description to table description with silly type converting
retype :: ModelDesc -> Either String TableDesc
retype (ModelDesc nm fs) = TableDesc (nm ++ "tbl") nm [] <$> (nub <$> mapM retype' fs) where
    retype' (ModelField fname (Just sqltype) _ _) = return $ TableColumn fname sqltype
    retype' (ModelField fname Nothing Nothing _) = return $ TableColumn fname "text"
    retype' (ModelField fname Nothing (Just ftype) _) = TableColumn fname <$> maybe unknown Right (lookup ftype retypes) where
        unknown = Left $ "Unknown type: " ++ ftype
    retypes = [
        ("datetime", "timestamp"),
        ("dictionary", "text"),
        ("phone", "text"),
        ("checkbox", "bool"),
        ("coords", "geometry(point,4326)"),
        ("date", "timestamp"),
        ("picker", "text"),
        ("map", "text"),
        ("textarea", "text"),
        ("reference", "text"),
        ("files", "text"),
        ("json", "text"),
        ("partnerTable", "text"),
        ("statictext", "text"),
        ("dictionary-many", "text[]" )]

-- | Add id column
addId :: TableDesc -> TableDesc
addId = addColumn "id" "integer"

-- | Add column
addColumn :: String -> String -> TableDesc -> TableDesc
addColumn nm tp (TableDesc n mdl h fs) = TableDesc n mdl h $ (TableColumn nm tp) : fs

-- | Services
services :: [String]
services = [
    "averageCommissioner",
    "bank",
    "consultation",
    "continue",
    "deliverCar",
    "deliverClient",
    "deliverParts",
    "hotel",
    "information",
    "insurance",
    "ken",
    "rent",
    "sober",
    "taxi",
    "tech",
    "tech1",
    "tickets",
    "towage",
    "transportation"]

-- | Make service table and inherit services from it
mergeServices :: [TableDesc] -> [TableDesc]
mergeServices tbls = srvBase : (srvsInherited ++ notSrvs) where
    srvBase = addColumn "type" "text" $ TableDesc "servicetbl" "service" [] srvBaseFields
    srvsInherited = map inherit srvs
    inherit (TableDesc nm mdl inhs fs) = TableDesc nm mdl (srvBase : inhs) $ fs \\ srvBaseFields

    (srvs, notSrvs) = partition ((`elem` (map addTbl services)) . tableName) tbls
    srvBaseFields = foldr1 intersect $ map tableFields srvs

    addTbl nm = nm ++ "tbl"

-- | Find table for model
tableByModel :: (Eq s, IsString s) => s -> [TableDesc] -> Maybe TableDesc
tableByModel name = find ((== name) . fromString . tableModel)

-- | WORKAROUND: Explicitly add type value for data in service-derived model
addType :: TableDesc -> M.Map C8.ByteString C8.ByteString -> M.Map C8.ByteString C8.ByteString
addType (TableDesc _ mdl _ _) dat = if mdl `elem` services then M.insert "type" (fromString mdl) dat else dat

str :: C8.ByteString -> String
str = T.unpack . T.decodeUtf8

unstr :: String -> C8.ByteString
unstr = T.encodeUtf8 . T.pack

-- | Convert data accord to its types
typize :: TableDesc -> M.Map C8.ByteString C8.ByteString -> M.Map C8.ByteString P.Action
typize tbl = M.mapWithKey convertData where
    convertData k v = fromMaybe (P.toField v) $ do
        t <- fmap columnType $ find ((== (str k)) . columnName) $ tableFlatFields tbl
        conv <- lookup t convertors
        case conv v of
            Left err -> return $ P.toField P.Null
            Right x -> return x

    convertors :: [(String, C8.ByteString -> Either String P.Action)]
    convertors = [
        ("text", Right . P.toField),
        ("bool", fromB),
        ("integer", fromI),
        ("geometry(point,4326)", fromCoords),
        ("timestamp", fromPosix),
        ("text[]", toArray)]

    fromB :: C8.ByteString -> Either String P.Action
    fromB "" = Right $ P.toField P.Null
    fromB "1" = Right $ P.toField True
    fromB "0" = Right $ P.toField False
    fromB v = Left $ "Not a boolean: " ++ str v

    fromI :: C8.ByteString -> Either String P.Action
    fromI "" = Right $ P.toField P.Null
    fromI v = do
        x <- asInt v
        return $ P.toField x

    fromPosix :: C8.ByteString -> Either String P.Action
    fromPosix "" = Right $ P.toField P.Null
    fromPosix v = do
        x <- asInt v
        return $ P.toField $ utcToLocalTime utc
            $ posixSecondsToUTCTime $ fromInteger x

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

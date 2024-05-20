module Beam (main) where

import Database.Beam ((==.), (||?.))
import System.Environment (getEnv)

import Control.Monad.Free.Church
import Data.ByteString (ByteString)
import qualified Data.ByteString as BL
import Data.ByteString.Builder (byteString, toLazyByteString)
import qualified Data.ByteString.Char8 as BS8
import Data.Text
import qualified Database.Beam as B
import qualified Database.Beam.Postgres as B
import qualified Database.Beam.Postgres.Full as B
import qualified Database.Beam.Postgres.Syntax as B
import qualified Database.PostgreSQL.LibPQ as LibPQI
import qualified Database.PostgreSQL.Simple as PQ
import qualified Database.PostgreSQL.Simple.Internal as PQI
import qualified Types.Storage.Configuration as SC
import qualified Types.Storage.DB as DB

dbTable :: Text -> B.DatabaseEntity be DB.TestDB (B.TableEntity SC.ConfigurationT)
dbTable = DB.configuration . DB.testDB "gcp_Configurations"

selectOneMaybe ::
  ( B.Beamable table
  , B.Database be db
  , B.FromBackendRow be (table B.Identity)
  , be ~ B.Postgres
  ) =>
  PQ.Connection ->
  B.DatabaseEntity be db (B.TableEntity table) ->
  (table (B.QExpr be B.QBaseScope) -> B.QExpr be B.QBaseScope B.SqlBool) ->
  (table (B.QExpr be B.QBaseScope) -> B.QExpr be B.QBaseScope B.SqlBool) ->
  -- IO (Maybe (table B.Identity))
  IO ()
selectOneMaybe conn table predicate predicate2 = do
  let query = B.select $ B.filter_' predicate $ B.all_ table
  let syntax = pgStmtSyntax query
  renderedSyntax <- PQI.withConnection conn $ \c -> traverse (pgRenderSyntax c) syntax
  B.pgTraceStmtIO conn query

-- print renderedSyntax
-- B.runBeamPostgresDebug (const $ pure ()) conn (B.runSelectReturningOne query)

main :: IO ()
main = do
  connectionString <- getEnv "DATABASE_CONNSTRING"
  conn <- PQ.connectPostgreSQL (BS8.pack connectionString)
  let filterBy SC.Configuration{key = key} =
        B.sqlBool_ (key ==. B.val_ "PassettoKeyId")
          ||?. B.sqlBool_ (key ==. B.val_ "NotPassettoKeyId")
  let filterBy2 SC.Configuration{key = key} = B.sqlBool_ (B.in_ key (B.val_ <$> ["?"]))
  -- let filterBy2 = B.parameter_ "Helo"
  config <- selectOneMaybe conn (dbTable "public") filterBy filterBy2
  -- print config
  pure ()

-- parsed exp@(B.Expression ex) = print exp

-----------
-- beam-postgres
-- module Database.Beam.Postgres.Debug
class CustomPgDebugStmt statement where
  pgStmtSyntax :: statement -> Maybe B.PgSyntax

instance CustomPgDebugStmt (B.SqlSelect B.Postgres a) where
  pgStmtSyntax (B.SqlSelect (B.PgSelectSyntax e)) = Just e
instance CustomPgDebugStmt (B.SqlInsert B.Postgres a) where
  pgStmtSyntax B.SqlInsertNoRows = Nothing
  pgStmtSyntax (B.SqlInsert _ (B.PgInsertSyntax e)) = Just e
instance CustomPgDebugStmt (B.SqlUpdate B.Postgres a) where
  pgStmtSyntax B.SqlIdentityUpdate = Nothing
  pgStmtSyntax (B.SqlUpdate _ (B.PgUpdateSyntax e)) = Just e
instance CustomPgDebugStmt (B.SqlDelete B.Postgres a) where
  pgStmtSyntax (B.SqlDelete _ (B.PgDeleteSyntax e)) = Just e
instance CustomPgDebugStmt (B.PgInsertReturning a) where
  pgStmtSyntax B.PgInsertReturningEmpty = Nothing
  pgStmtSyntax (B.PgInsertReturning e) = Just e
instance CustomPgDebugStmt (B.PgUpdateReturning a) where
  pgStmtSyntax B.PgUpdateReturningEmpty = Nothing
  pgStmtSyntax (B.PgUpdateReturning e) = Just e
instance CustomPgDebugStmt (B.PgDeleteReturning a) where
  pgStmtSyntax (B.PgDeleteReturning e) = Just e

-----------

-----------
-- beam-postgres
-- module Database.Beam.Postgres.Connection
pgRenderSyntax ::
  LibPQI.Connection -> B.PgSyntax -> IO ByteString
pgRenderSyntax conn (B.PgSyntax mkQuery) =
  renderBuilder <$> runF mkQuery finish step mempty
 where
  renderBuilder = BL.toStrict . toLazyByteString
  step (B.EmitBuilder b next) a = do
    -- putStrLn $ "EmitBuilder a: " <> show a
    -- putStrLn $ "EmitBuilder b: " <> show  b
    next (a <> b)
  step (B.EmitByteString b next) a = do
    -- putStrLn $ "EmitByteString a: " <> show  a
    -- putStrLn $ "EmitByteString b: " <> show  b
    next (a <> byteString b)
  step (B.EscapeString b next) a = do
    -- putStrLn $ "EscapeString a: " <> show  a
    -- putStrLn $ "EscapeString b: " <> show  b
    res <- wrapError "EscapeString" (LibPQI.escapeStringConn conn b)
    next (a <> byteString res)
  step (B.EscapeBytea b next) a = do
    -- putStrLn $ "EscapeBytea a: " <> show  a
    -- putStrLn $ "EscapeBytea b: " <> show  b
    res <- wrapError "EscapeBytea" (LibPQI.escapeByteaConn conn b)
    next (a <> byteString res)
  step (B.EscapeIdentifier b next) a = do
    -- putStrLn $ "EscapeIdentifier a: " <> show  a
    -- putStrLn $ "EscapeIdentifier b: " <> show  b
    res <- wrapError "EscapeIdentifier" (LibPQI.escapeIdentifier conn b)
    next (a <> byteString res)

  finish _ = pure

  wrapError' step' go = do
    res <- go
    case res of
      Right res' -> pure res'
      Left res' -> fail (step' <> ": " <> show res')

  wrapError step' go = do
    res <- go
    case res of
      Just res' -> pure res'
      Nothing -> fail (step' <> ": " <> show res)

-----------

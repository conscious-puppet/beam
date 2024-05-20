{-# LANGUAGE TypeApplications #-}

module Beam (main) where

import Database.Beam ((==*.), (==.), (||?.))
import System.Environment (getEnv)

import Control.Monad.Free.Church
import Data.ByteString (ByteString)
import qualified Data.ByteString as BL
import Data.ByteString.Builder (byteString, toLazyByteString)
import qualified Data.ByteString.Char8 as BS8
import Data.Text
import qualified Database.Beam as B
import qualified Database.Beam.Postgres.AST as B
import qualified Database.Beam.Postgres.FullP as B
import qualified Database.Beam.PostgresP as B
import qualified Database.PostgreSQL.LibPQ as LibPQI
import qualified Database.PostgreSQL.Simple as PQ
import qualified Database.PostgreSQL.Simple.Internal as PQI
import qualified Database.PostgreSQL.Simple.TypeInfo.Static as PQ
import qualified Types.Storage.Configuration as SC
import qualified Types.Storage.DB as DB

dbTable :: Text -> B.DatabaseEntity be DB.TestDB (B.TableEntity SC.ConfigurationT)
dbTable = DB.configuration . DB.testDB "gcp_Configurations"

-- selectOneMaybe ::
--   ( B.Beamable table
--   , B.Database be db
--   , B.FromBackendRow be (table B.Identity)
--   , be ~ B.PostgresP
--   ) =>
--   PQ.Connection ->
--   B.DatabaseEntity be db (B.TableEntity table) ->
--   (table (B.QExpr be B.QBaseScope) -> B.QExpr be B.QBaseScope B.SqlBool) ->
--   IO (Maybe (table B.Identity))
selectOneMaybe conn table predicate = do
  let query = B.select $ B.filter_' predicate $ B.all_ table
  B.runBeamPostgresDebug putStrLn conn (B.runSelectReturningOne query)

main :: IO ()
main = do
  connectionString <- getEnv "DATABASE_CONNSTRING"
  conn <- PQ.connectPostgreSQL (BS8.pack connectionString)
  let filterBy SC.Configuration{key = key} =
        B.sqlBool_ (key ==. B.val_ "PassettoKeyId")
          ||?. B.sqlBool_ (key ==. B.val_ "NotPassettoKeyId")
          ||?. (key ==*. B.anyIn_ [B.val_ "PassettoKeyId"])
  config <- selectOneMaybe conn (dbTable "public") filterBy
  print config

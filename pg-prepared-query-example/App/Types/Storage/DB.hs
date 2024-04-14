module Types.Storage.DB where

import Data.Text
import qualified Database.Beam as B
import qualified Types.Storage.Configuration as Configuration

data TestDB entity = TestDB
  {configuration :: entity (B.TableEntity Configuration.ConfigurationT)}
  deriving (B.Generic, B.Database be)

testDB :: Text -> Text -> B.DatabaseSettings be TestDB
testDB tableName schema =
  B.defaultDbSettings
    `B.withDbModification` B.dbModification
      { configuration = Configuration.configurationEMod tableName schema
      }

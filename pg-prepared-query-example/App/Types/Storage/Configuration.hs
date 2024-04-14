module Types.Storage.Configuration where

import Data.Int (Int32)
import Data.Text (Text)
import Database.Beam (Beamable, Columnar, Generic, Identity, Table (..))

import Data.Aeson
import Data.Time.LocalTime

import qualified Database.Beam as B
import qualified Database.Beam.Schema.Tables as B
import Prelude hiding (id)

data ConfigurationT f = Configuration
  { id :: B.C f Text
  , key :: B.C f Text
  , value :: B.C f Text
  , createdAt :: B.C f LocalTime
  , updatedAt :: B.C f LocalTime
  }
  deriving (Generic, B.Beamable)

type Configuration = ConfigurationT Identity

type ConfigurationPrimaryKey = B.PrimaryKey ConfigurationT Identity

instance B.Table ConfigurationT where
  data PrimaryKey ConfigurationT f = ConfigurationPrimaryKey (B.C f Text) deriving (Generic, B.Beamable)
  primaryKey = ConfigurationPrimaryKey . id

deriving instance Show Configuration
deriving instance Eq Configuration
deriving instance FromJSON Configuration
deriving instance ToJSON Configuration

configurationEMod :: Text -> Text -> B.EntityModification (B.DatabaseEntity be db) be (B.TableEntity ConfigurationT)
configurationEMod tableName schema =
  B.setEntitySchema (Just schema)
    <> B.setEntityName tableName
    <> B.modifyTableFields
      B.tableModification
        { id = "id"
        , key = "key"
        , value = "value"
        , createdAt = "createdAt"
        , updatedAt = "updatedAt"
        }

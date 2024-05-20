{-# OPTIONS_GHC -fno-warn-orphans #-}

{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}

module Database.Beam.Postgres.TypesP
  ( PostgresP(..)
  , fromPgIntegral
  , fromPgScientificOrIntegral
  ) where

import           Database.Beam hiding (DataType (..))
import           Database.Beam.Backend
import           Database.Beam.Backend.Internal.Compat
import           Database.Beam.Migrate.Generics
import           Database.Beam.Migrate.SQL (BeamMigrateOnlySqlBackend)
import           Database.Beam.Postgres.AST
import           Database.Beam.Query.SQL92

import qualified Database.PostgreSQL.Simple.FromField as Pg
import qualified Database.PostgreSQL.Simple.HStore as Pg (HStoreMap, HStoreList)
import qualified Database.PostgreSQL.Simple.Types as Pg
import qualified Database.PostgreSQL.Simple.Range as Pg (PGRange)
import qualified Database.PostgreSQL.Simple.Time as Pg (Date, UTCTimestamp, ZonedTimestamp, LocalTimestamp)

import           Data.Aeson hiding (Value)
import qualified Data.Aeson as Aeson
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BL
import           Data.CaseInsensitive (CI)
import           Data.Int
import           Data.Proxy
import           Data.Ratio (Ratio)
import           Data.Scientific (Scientific, toBoundedInteger)
import           Data.Tagged
import           Data.Text (Text)
import qualified Data.Text.Lazy as TL
import           Data.Time (UTCTime, Day, TimeOfDay, LocalTime, NominalDiffTime, ZonedTime(..))
import           Data.UUID.Types (UUID)
import           Data.Vector (Vector)
import           Data.Word
import           GHC.TypeLits

-- | The Postgres backend type, used to parameterize 'MonadBeam'. See the
-- definitions there for more information. The corresponding query monad is
-- 'Pg'. See documentation for 'MonadBeam' and the
-- <https://haskell-beam.github/beam/ user guide> for more information on using
-- this backend.
data PostgresP
  = PostgresP

instance BeamBackend PostgresP where
  type BackendFromField PostgresP = Pg.FromField

instance HasSqlInTable PostgresP where

instance Pg.FromField SqlNull where
  fromField field d = fmap (\Pg.Null -> SqlNull) (Pg.fromField field d)

-- | Deserialize integral fields, possibly downcasting from a larger numeric type
-- via 'Scientific' if we won't lose data, and then falling back to any integral
-- type via 'Integer'
fromPgScientificOrIntegral :: (Bounded a, Integral a) => FromBackendRowM PostgresP a
fromPgScientificOrIntegral = do
  sciVal <- fmap (toBoundedInteger =<<) peekField
  case sciVal of
    Just sciVal' -> do
      pure sciVal'
    Nothing -> fromIntegral <$> fromBackendRow @PostgresP @Integer

-- | Deserialize integral fields, possibly downcasting from a larger integral
-- type, but only if we won't lose data
fromPgIntegral :: forall a
                . (Pg.FromField a, Integral a, Typeable a)
               => FromBackendRowM PostgresP a
fromPgIntegral = do
  val <- peekField
  case val of
    Just val' -> do
      pure val'
    Nothing -> do
      val' <- parseOneField @PostgresP @Integer
      let val'' = fromIntegral val'
      if fromIntegral val'' == val'
        then pure val''
        else fail (concat [ "Data loss while downsizing Integral type. "
                          , "Make sure your Haskell types are wide enough for your data" ])

-- Default FromBackendRow instances for all postgresql-simple FromField instances
instance FromBackendRow PostgresP SqlNull
instance FromBackendRow PostgresP Bool
instance FromBackendRow PostgresP Char
instance FromBackendRow PostgresP Double
instance FromBackendRow PostgresP Int16 where
  fromBackendRow = fromPgIntegral
instance FromBackendRow PostgresP Int32 where
  fromBackendRow = fromPgIntegral
instance FromBackendRow PostgresP Int64 where
  fromBackendRow = fromPgIntegral

instance TypeError (PreferExplicitSize Int Int32) => FromBackendRow PostgresP Int where
  fromBackendRow = fromPgIntegral

-- Word values are serialized as SQL @NUMBER@ types to guarantee full domain coverage.
-- However, we want them te be serialized/deserialized as whichever type makes sense
instance FromBackendRow PostgresP Word16 where
  fromBackendRow = fromPgScientificOrIntegral
instance FromBackendRow PostgresP Word32 where
  fromBackendRow = fromPgScientificOrIntegral
instance FromBackendRow PostgresP Word64 where
  fromBackendRow = fromPgScientificOrIntegral

instance TypeError (PreferExplicitSize Word Word32) => FromBackendRow PostgresP Word where
  fromBackendRow = fromPgScientificOrIntegral

instance FromBackendRow PostgresP Integer
instance FromBackendRow PostgresP ByteString
instance FromBackendRow PostgresP Scientific
instance FromBackendRow PostgresP BL.ByteString
instance FromBackendRow PostgresP Text
instance FromBackendRow PostgresP UTCTime
instance FromBackendRow PostgresP Aeson.Value
instance FromBackendRow PostgresP TL.Text
instance FromBackendRow PostgresP Pg.Oid
instance FromBackendRow PostgresP LocalTime where
  fromBackendRow =
    peekField >>=
    \case
      Just (x :: LocalTime) -> pure x

      -- Also accept 'TIMESTAMP WITH TIME ZONE'. Considered as
      -- 'LocalTime', because postgres always returns times in the
      -- server timezone, regardless of type.
      Nothing ->
        peekField >>=
        \case
          Just (x :: ZonedTime) -> pure (zonedTimeToLocalTime x)
          Nothing -> fail "'TIMESTAMP WITH TIME ZONE' or 'TIMESTAMP WITHOUT TIME ZONE' required for LocalTime"
instance FromBackendRow PostgresP TimeOfDay
instance FromBackendRow PostgresP Day
instance FromBackendRow PostgresP UUID
instance FromBackendRow PostgresP Pg.Null
instance FromBackendRow PostgresP Pg.Date
instance FromBackendRow PostgresP Pg.ZonedTimestamp
instance FromBackendRow PostgresP Pg.UTCTimestamp
instance FromBackendRow PostgresP Pg.LocalTimestamp
instance FromBackendRow PostgresP Pg.HStoreMap
instance FromBackendRow PostgresP Pg.HStoreList
instance FromBackendRow PostgresP [Char]
instance FromBackendRow PostgresP (Ratio Integer)
instance FromBackendRow PostgresP (CI Text)
instance FromBackendRow PostgresP (CI TL.Text)
instance (Pg.FromField a, Typeable a) => FromBackendRow PostgresP (Vector a) where
  fromBackendRow = do
      isNull <- peekField
      case isNull of
        Just SqlNull -> pure mempty
        Nothing -> parseOneField @PostgresP @(Vector a)
instance (Pg.FromField a, Typeable a) => FromBackendRow PostgresP (Pg.PGArray a)
instance FromBackendRow PostgresP (Pg.Binary ByteString)
instance FromBackendRow PostgresP (Pg.Binary BL.ByteString)
instance (Pg.FromField a, Typeable a) => FromBackendRow PostgresP (Pg.PGRange a)
instance (Pg.FromField a, Pg.FromField b, Typeable a, Typeable b) => FromBackendRow PostgresP (Either a b)

instance BeamSqlBackend PostgresP
instance BeamMigrateOnlySqlBackend PostgresP
type instance BeamSqlBackendSyntax PostgresP = Command

instance BeamSqlBackendIsString PostgresP String
instance BeamSqlBackendIsString PostgresP Text

instance HasQBuilder PostgresP where
  buildSqlQuery = buildSql92Query' True

-- * Instances for 'HasDefaultSqlDataType'

instance HasDefaultSqlDataType PostgresP ByteString where
  defaultSqlDataType _ _ _ = DataTypeBytea

instance HasDefaultSqlDataType PostgresP LocalTime where
  defaultSqlDataType _ _ _ = timestampType Nothing False

instance HasDefaultSqlDataType PostgresP UTCTime where
  defaultSqlDataType _ _ _ = timestampType Nothing True

instance HasDefaultSqlDataType PostgresP (SqlSerial Int16) where
  defaultSqlDataType _ _ False = DataTypeSmallSerial
  defaultSqlDataType _ _ _ = smallIntType

instance HasDefaultSqlDataType PostgresP (SqlSerial Int32) where
  defaultSqlDataType _ _ False = DataTypeSerial
  defaultSqlDataType _ _ _ = intType

instance HasDefaultSqlDataType PostgresP (SqlSerial Int64) where
  defaultSqlDataType _ _ False = DataTypeBigSerial
  defaultSqlDataType _ _ _ = bigIntType

instance TypeError (PreferExplicitSize Int Int32) => HasDefaultSqlDataType PostgresP (SqlSerial Int) where
  defaultSqlDataType _ = defaultSqlDataType (Proxy @(SqlSerial Int32))

instance HasDefaultSqlDataType PostgresP UUID where
  defaultSqlDataType _ _ _ = DataTypeUUID

-- * Instances for 'HasSqlEqualityCheck'

#define PG_HAS_EQUALITY_CHECK(ty)                                 \
  instance HasSqlEqualityCheck PostgresP (ty);           \
  instance HasSqlQuantifiedEqualityCheck PostgresP (ty);

PG_HAS_EQUALITY_CHECK(Bool)
PG_HAS_EQUALITY_CHECK(Double)
PG_HAS_EQUALITY_CHECK(Float)
PG_HAS_EQUALITY_CHECK(Int8)
PG_HAS_EQUALITY_CHECK(Int16)
PG_HAS_EQUALITY_CHECK(Int32)
PG_HAS_EQUALITY_CHECK(Int64)
PG_HAS_EQUALITY_CHECK(Integer)
PG_HAS_EQUALITY_CHECK(Word8)
PG_HAS_EQUALITY_CHECK(Word16)
PG_HAS_EQUALITY_CHECK(Word32)
PG_HAS_EQUALITY_CHECK(Word64)
PG_HAS_EQUALITY_CHECK(Text)
PG_HAS_EQUALITY_CHECK(TL.Text)
PG_HAS_EQUALITY_CHECK(UTCTime)
PG_HAS_EQUALITY_CHECK(Aeson.Value)
PG_HAS_EQUALITY_CHECK(Pg.Oid)
PG_HAS_EQUALITY_CHECK(LocalTime)
PG_HAS_EQUALITY_CHECK(ZonedTime)
PG_HAS_EQUALITY_CHECK(TimeOfDay)
PG_HAS_EQUALITY_CHECK(NominalDiffTime)
PG_HAS_EQUALITY_CHECK(Day)
PG_HAS_EQUALITY_CHECK(UUID)
PG_HAS_EQUALITY_CHECK([Char])
PG_HAS_EQUALITY_CHECK(Pg.HStoreMap)
PG_HAS_EQUALITY_CHECK(Pg.HStoreList)
PG_HAS_EQUALITY_CHECK(Pg.Date)
PG_HAS_EQUALITY_CHECK(Pg.ZonedTimestamp)
PG_HAS_EQUALITY_CHECK(Pg.LocalTimestamp)
PG_HAS_EQUALITY_CHECK(Pg.UTCTimestamp)
PG_HAS_EQUALITY_CHECK(Scientific)
PG_HAS_EQUALITY_CHECK(ByteString)
PG_HAS_EQUALITY_CHECK(BL.ByteString)
PG_HAS_EQUALITY_CHECK(Vector a)
PG_HAS_EQUALITY_CHECK(CI Text)
PG_HAS_EQUALITY_CHECK(CI TL.Text)

instance TypeError (PreferExplicitSize Int Int32) => HasSqlEqualityCheck PostgresP Int
instance TypeError (PreferExplicitSize Int Int32) => HasSqlQuantifiedEqualityCheck PostgresP Int
instance TypeError (PreferExplicitSize Word Word32) => HasSqlEqualityCheck PostgresP Word
instance TypeError (PreferExplicitSize Word Word32) => HasSqlQuantifiedEqualityCheck PostgresP Word

instance HasSqlEqualityCheck PostgresP a =>
  HasSqlEqualityCheck PostgresP (Tagged t a)
instance HasSqlQuantifiedEqualityCheck PostgresP a =>
  HasSqlQuantifiedEqualityCheck PostgresP (Tagged t a)

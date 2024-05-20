{-# LANGUAGE CPP #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}

-- | This module implements an AST type for SQL92. It allows us to realize
--   the call structure of the builders defined in "Database.Beam.Backend.SQL.SQL92"
module Database.Beam.Postgres.AST where

import Prelude hiding (Ordering)

import Database.Beam.Backend.Internal.Compat
import Database.Beam.Backend.SQL.SQL92
import Database.Beam.Backend.SQL.SQL99
import Database.Beam.Backend.SQL.SQL2003
import Database.Beam.Backend.SQL.Types

import Data.Text (Text)
import Data.ByteString (ByteString)
import Data.Time
import Data.Word
import Data.Typeable
import Data.Int
import GHC.TypeLits

import Data.Functor.Classes

import           Database.Beam hiding (insert, ExtractField(..), DataType(..))
import           Database.Beam.Backend.Internal.Compat
import           Database.Beam.Backend.SQL
import           Database.Beam.Migrate
import           Database.Beam.Migrate.SQL.Builder hiding (fromSqlConstraintAttributes)
import           Database.Beam.Migrate.Serialization

import           Control.Monad (guard)
import           Control.Monad.Free
import           Control.Monad.Free.Church


------
import           Data.Aeson (Value, object, (.=))
import           Data.Bits
import           Data.ByteString (ByteString)
import           Data.ByteString.Builder (Builder, byteString, char8, toLazyByteString)
import qualified Data.ByteString.Char8 as B
import           Data.ByteString.Lazy.Char8 (toStrict)
import qualified Data.ByteString.Lazy.Char8 as BL
import           Data.CaseInsensitive (CI)
import qualified Data.CaseInsensitive as CI
import           Data.Coerce
import           Data.Functor.Classes
import           Data.Hashable
import           Data.Int
import           Data.Maybe
#if !MIN_VERSION_base(4, 11, 0)
import           Data.Semigroup
#endif
import           Data.Scientific (Scientific)
import           Data.String (IsString(..), fromString)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Lazy as TL
import           Data.Time (LocalTime, UTCTime, TimeOfDay, NominalDiffTime, Day)
import           Data.UUID.Types (UUID, toASCIIBytes)
import           Data.Word
import qualified Data.Vector as V
import           GHC.TypeLits

import           Data.Hashable
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Lazy as TL

import qualified Database.PostgreSQL.Simple.ToField as Pg
import qualified Database.PostgreSQL.Simple.TypeInfo.Static as Pg
import qualified Database.PostgreSQL.Simple.Types as Pg (Oid(..), Binary(..), Null(..))
import qualified Database.PostgreSQL.Simple.Time as Pg (Date, LocalTimestamp, UTCTimestamp)
import qualified Database.PostgreSQL.Simple.HStore as Pg (HStoreList, HStoreMap, HStoreBuilder)
------


data PostgresInaccessible

data PgSelectLockingStrength
  = PgSelectLockingStrengthUpdate
  -- ^ @UPDATE@
  | PgSelectLockingStrengthNoKeyUpdate
  -- ^ @NO KEY UPDATE@
  | PgSelectLockingStrengthShare
  -- ^ @SHARE@
  | PgSelectLockingStrengthKeyShare
  -- ^ @KEY SHARE@
  deriving (Show, Eq, Generic)

-- | Specifies how we should handle lock conflicts.
--
-- See
-- <https://www.postgresql.org/docs/9.5/static/sql-select.html#SQL-FOR-UPDATE-SHARE the manual section>
-- for more information
data PgSelectLockingOptions
  = PgSelectLockingOptionsNoWait
  -- ^ @NOWAIT@. Report an error rather than waiting for the lock
  | PgSelectLockingOptionsSkipLocked
  -- ^ @SKIP LOCKED@. Rather than wait for a lock, skip the row instead
  deriving (Show, Eq, Generic)

newtype PgInsertOnConflictSyntax = PgInsertOnConflictSyntax { fromPgInsertOnConflict :: PgSyntax }


-- Rename this to PgCommandBuilder
data Command
  = SelectCommand Select
  | InsertCommand Insert
  | UpdateCommand Update
  | DeleteCommand Delete
  | CreateTableCommand CreateTable
  | DropTableCommand DropTable
  | AlterTableCommand AlterTable
  deriving (Show, Eq)

data PgSyntaxF f where
  EmitByteString :: ByteString -> f -> PgSyntaxF f
  EmitCommandBuilder :: Command -> f -> PgSyntaxF f
  EmitBuilder :: Builder -> f -> PgSyntaxF f

  EscapeString :: ByteString -> f -> PgSyntaxF f
  EscapeBytea  :: ByteString -> f -> PgSyntaxF f
  EscapeIdentifier :: ByteString -> f -> PgSyntaxF f
deriving instance Functor PgSyntaxF

instance Eq1 PgSyntaxF where
  liftEq eq (EmitByteString b1 next1) (EmitByteString b2 next2) =
      b1 == b2 && next1 `eq` next2
  liftEq eq (EmitBuilder b1 next1) (EmitBuilder b2 next2) =
      toLazyByteString b1 == toLazyByteString b2 && next1 `eq` next2
  liftEq eq (EmitCommandBuilder b1 next1) (EmitCommandBuilder b2 next2) =
      b1 == b2 && next1 `eq` next2
  liftEq eq (EscapeString b1 next1) (EscapeString b2 next2) =
      b1 == b2 && next1 `eq` next2
  liftEq eq (EscapeBytea b1 next1) (EscapeBytea b2 next2) =
      b1 == b2 && next1 `eq` next2
  liftEq eq (EscapeIdentifier b1 next1) (EscapeIdentifier b2 next2) =
      b1 == b2 && next1 `eq` next2
  liftEq _ _ _ = False

instance Eq f => Eq (PgSyntaxF f) where
  (==) = eq1


type PgSyntaxM = F PgSyntaxF

newtype PgSyntax
  = PgSyntax { buildPgSyntax :: PgSyntaxM () }

instance Semigroup PgSyntax where
  a <> b = PgSyntax (buildPgSyntax a >> buildPgSyntax b)

instance Monoid PgSyntax where
  mempty = PgSyntax (pure ())
  mappend = (<>)

instance Eq PgSyntax where
  PgSyntax x == PgSyntax y = (fromF x :: Free PgSyntaxF ()) == fromF y

emit :: ByteString -> PgSyntax
emit bs = PgSyntax (liftF (EmitByteString bs ()))

-- mytodo: resolve this
emitBuilder :: Builder -> PgSyntax
emitBuilder b = PgSyntax (liftF (EmitBuilder b ()))

emitCommandBuilder :: Command -> PgSyntax
emitCommandBuilder b = PgSyntax (liftF (EmitCommandBuilder b ()))

escapeString, escapeBytea, escapeIdentifier :: ByteString -> PgSyntax
escapeString bs = PgSyntax (liftF (EscapeString bs ()))
escapeBytea bin = PgSyntax (liftF (EscapeBytea bin ()))
escapeIdentifier id = PgSyntax (liftF (EscapeIdentifier id ()))

nextSyntaxStep :: PgSyntaxF f -> f
nextSyntaxStep (EmitByteString _ next) = next
nextSyntaxStep (EmitBuilder _ next) = next
nextSyntaxStep (EscapeString _ next) = next
nextSyntaxStep (EscapeBytea _ next) = next
nextSyntaxStep (EscapeIdentifier _ next) = next

-- mytodo: derive hashable for command
-- instance Hashable PgSyntax where
--   hashWithSalt salt (PgSyntax s) = runF s finish step salt
--     where
--       finish _ salt = hashWithSalt salt ()
--       step (EmitByteString b hashRest) salt = hashRest (hashWithSalt salt (0 :: Int, b))
--       step (EmitBuilder b hashRest)    salt = hashRest (hashWithSalt salt (1 :: Int, hashWithSalt salt (1 :: Int, b)))
--       step (EscapeString  b hashRest)  salt = hashRest (hashWithSalt salt (2 :: Int, b))
--       step (EscapeBytea  b hashRest)   salt = hashRest (hashWithSalt salt (3 :: Int, b))
--       step (EscapeIdentifier b hashRest) salt = hashRest (hashWithSalt salt (4 :: Int, b))

-- instance Sql92DisplaySyntax PgSyntax where
--   displaySyntax = BL.unpack . pgRenderSyntaxScript

instance IsSql92Syntax Command where
  type Sql92SelectSyntax Command = Select
  type Sql92UpdateSyntax Command = Update
  type Sql92InsertSyntax Command = Insert
  type Sql92DeleteSyntax Command = Delete

  selectCmd = SelectCommand
  insertCmd = InsertCommand
  updateCmd = UpdateCommand
  deleteCmd = DeleteCommand

-- mytodo: create types for these
data CreateTable = CreateTable deriving (Show, Eq)
data DropTable = DropTable  deriving (Show, Eq)
data AlterTable = AlterTable deriving (Show, Eq)

instance IsSql92DdlCommandSyntax Command where
  type Sql92DdlCommandCreateTableSyntax Command = CreateTable
  type Sql92DdlCommandDropTableSyntax Command = DropTable
  type Sql92DdlCommandAlterTableSyntax Command = AlterTable

  createTableCmd = undefined
  dropTableCmd   = undefined
  alterTableCmd  = undefined

instance IsSql92DropTableSyntax DropTable where
  type Sql92DropTableTableNameSyntax DropTable = TableName

  dropTableSyntax tblNm = undefined

data ColumnSchema  = ColumnSchema deriving (Eq, Show)

instance Hashable ColumnSchema where
  hashWithSalt salt = undefined

instance Sql92DisplaySyntax ColumnSchema where
  displaySyntax = undefined

data TableConstraint = TableConstraint deriving (Eq, Show)
data TableOptions = TableOptions deriving (Eq, Show)

instance IsSql92CreateTableSyntax CreateTable where
  type Sql92CreateTableTableNameSyntax CreateTable = TableName
  type Sql92CreateTableColumnSchemaSyntax CreateTable = ColumnSchema
  type Sql92CreateTableTableConstraintSyntax CreateTable = TableConstraint
  type Sql92CreateTableOptionsSyntax CreateTable = TableOptions

  createTableSyntax options tblNm fieldTypes constraints = undefined

instance IsSql92TableConstraintSyntax TableConstraint where
  primaryKeyConstraintSyntax fieldNames = undefined

data ColumnConstraintDefinition = ColumnConstraintDefinition deriving (Eq, Show)

instance Hashable ColumnConstraintDefinition where
  hashWithSalt salt = undefined

instance Sql92DisplaySyntax ColumnConstraintDefinition where
  displaySyntax = undefined

instance Sql92SerializableConstraintDefinitionSyntax ColumnConstraintDefinition where
  serializeConstraint = undefined

instance IsSql92ColumnSchemaSyntax ColumnSchema where
  type Sql92ColumnSchemaColumnTypeSyntax ColumnSchema = DataType
  type Sql92ColumnSchemaExpressionSyntax ColumnSchema = Expression
  type Sql92ColumnSchemaColumnConstraintDefinitionSyntax ColumnSchema = ColumnConstraintDefinition

  columnSchemaSyntax colType defaultClause constraints collation = undefined

data ColumnConstraint = ColumnConstraint

instance IsSql92ColumnConstraintDefinitionSyntax ColumnConstraintDefinition where
  type Sql92ColumnConstraintDefinitionConstraintSyntax ColumnConstraintDefinition = ColumnConstraint
  type Sql92ColumnConstraintDefinitionAttributesSyntax ColumnConstraintDefinition = SqlConstraintAttributesBuilder

  constraintDefinitionSyntax nm constraint attrs = undefined

data MatchType = MatchType
data ReferentialAction = ReferentialAction

instance IsSql92ColumnConstraintSyntax ColumnConstraint where
  type Sql92ColumnConstraintMatchTypeSyntax ColumnConstraint = MatchType
  type Sql92ColumnConstraintReferentialActionSyntax ColumnConstraint = ReferentialAction
  type Sql92ColumnConstraintExpressionSyntax ColumnConstraint = Expression

  notNullConstraintSyntax = undefined
  uniqueColumnConstraintSyntax = undefined
  primaryKeyColumnConstraintSyntax = undefined
  checkColumnConstraintSyntax expr = undefined
  referencesConstraintSyntax tbl fields matchType onUpdate onDelete = undefined

instance IsSql92ReferentialActionSyntax ReferentialAction where
  referentialActionCascadeSyntax = undefined
  referentialActionNoActionSyntax = undefined
  referentialActionSetDefaultSyntax = undefined
  referentialActionSetNullSyntax = undefined


instance IsSql92MatchTypeSyntax MatchType where
  fullMatchSyntax = undefined
  partialMatchSyntax = undefined

data AlterTableAction = AlterTableAction

instance IsSql92AlterTableSyntax AlterTable where
  type Sql92AlterTableAlterTableActionSyntax AlterTable = AlterTableAction
  type Sql92AlterTableTableNameSyntax AlterTable = TableName


  alterTableSyntax tblNm action = undefined

data AlterColumnAction = AlterColumnAction

instance IsSql92AlterTableActionSyntax AlterTableAction where
  type Sql92AlterTableAlterColumnActionSyntax AlterTableAction = AlterColumnAction
  type Sql92AlterTableColumnSchemaSyntax AlterTableAction = ColumnSchema

  alterColumnSyntax colNm action = undefined

  addColumnSyntax colNm schema = undefined

  dropColumnSyntax colNm = undefined

  renameTableToSyntax newNm = undefined

  renameColumnToSyntax oldNm newNm = undefined

instance IsSql92AlterColumnActionSyntax AlterColumnAction where
  setNullSyntax = undefined
  setNotNullSyntax = undefined

data Select
    = Select
    { selectTable :: SelectTable
    , selectOrdering   :: [ Ordering ]
    , selectLimit, selectOffset :: Maybe Integer }
    deriving (Show, Eq)

instance IsSql92SelectSyntax Select where
  type Sql92SelectSelectTableSyntax Select = SelectTable
  type Sql92SelectOrderingSyntax Select = Ordering

  selectStmt = Select

data SelectTable
  = SelectTable
  { selectQuantifier :: Maybe SetQuantifier
  , selectProjection :: Projection
  , selectFrom       :: Maybe From
  , selectWhere      :: Maybe Expression
  , selectGrouping   :: Maybe Grouping
  , selectHaving     :: Maybe Expression }
  | UnionTables Bool SelectTable SelectTable
  | IntersectTables Bool SelectTable SelectTable
  | ExceptTable Bool SelectTable SelectTable
  deriving (Show, Eq)

instance IsSql92SelectTableSyntax SelectTable where
  type Sql92SelectTableSelectSyntax SelectTable = Select
  type Sql92SelectTableExpressionSyntax SelectTable = Expression
  type Sql92SelectTableProjectionSyntax SelectTable = Projection
  type Sql92SelectTableFromSyntax SelectTable = From
  type Sql92SelectTableGroupingSyntax SelectTable = Grouping
  type Sql92SelectTableSetQuantifierSyntax SelectTable = SetQuantifier

  selectTableStmt = SelectTable
  unionTables = UnionTables
  intersectTables = IntersectTables
  exceptTable = ExceptTable

data Insert
  = Insert
  { insertTable :: TableName
  , insertFields :: [ Text ]
  , insertValues :: InsertValues }
  deriving (Show, Eq)

instance IsSql92InsertSyntax Insert where
  type Sql92InsertValuesSyntax Insert = InsertValues
  type Sql92InsertTableNameSyntax Insert = TableName

  insertStmt = Insert

data InsertValues
  = InsertValues
  { insertValuesExpressions :: [ [ Expression ] ] }
  | InsertSelect
  { insertSelectStmt :: Select }
  deriving (Show, Eq)

instance IsSql92InsertValuesSyntax InsertValues where
  type Sql92InsertValuesExpressionSyntax InsertValues = Expression
  type Sql92InsertValuesSelectSyntax InsertValues = Select

  insertSqlExpressions = InsertValues
  insertFromSql = InsertSelect

data Update
  = Update
  { updateTable :: TableName
  , updateFields :: [ (FieldName, Expression) ]
  , updateWhere :: Maybe Expression }
  deriving (Show, Eq)

instance IsSql92UpdateSyntax Update where
  type Sql92UpdateTableNameSyntax Update = TableName
  type Sql92UpdateFieldNameSyntax Update = FieldName
  type Sql92UpdateExpressionSyntax Update = Expression

  updateStmt = Update

data Delete
  = Delete
  { deleteTable :: TableName
  , deleteAlias :: Maybe Text
  , deleteWhere :: Maybe Expression }
  deriving (Show, Eq)

instance IsSql92DeleteSyntax Delete where
  type Sql92DeleteTableNameSyntax Delete = TableName
  type Sql92DeleteExpressionSyntax Delete = Expression

  deleteStmt = Delete
  deleteSupportsAlias _ = True

data FieldName
  = QualifiedField Text Text
  | UnqualifiedField Text
  deriving (Show, Eq)

instance IsSql92FieldNameSyntax FieldName where
  qualifiedField = QualifiedField
  unqualifiedField = UnqualifiedField

data ComparatorQuantifier
  = ComparatorQuantifierAny
  | ComparatorQuantifierAll
  deriving (Show, Eq)

instance IsSql92QuantifierSyntax ComparatorQuantifier where
  quantifyOverAll = ComparatorQuantifierAll
  quantifyOverAny = ComparatorQuantifierAny

data ExtractField
  = ExtractFieldTimeZoneHour
  | ExtractFieldTimeZoneMinute

  | ExtractFieldDateTimeYear
  | ExtractFieldDateTimeMonth
  | ExtractFieldDateTimeDay
  | ExtractFieldDateTimeHour
  | ExtractFieldDateTimeMinute
  | ExtractFieldDateTimeSecond
  deriving (Show, Eq)

data DataType
  = DataTypeChar Bool {- Varying -} (Maybe Word) (Maybe Text)
  | DataTypeNationalChar Bool (Maybe Word)
  | DataTypeBit Bool (Maybe Word)
  | DataTypeNumeric (Maybe (Word, Maybe Word))
  | DataTypeDecimal (Maybe (Word, Maybe Word))
  | DataTypeInteger
  | DataTypeSmallInt
  | DataTypeBigInt
  | DataTypeFloat (Maybe Word)
  | DataTypeReal
  | DataTypeDoublePrecision
  | DataTypeDate
  | DataTypeTime (Maybe Word) {- time fractional seconds precision -} Bool {- With time zone -}
  | DataTypeTimeStamp (Maybe Word) Bool
  | DataTypeInterval ExtractField
  | DataTypeIntervalFromTo ExtractField ExtractField
  | DataTypeBoolean

  | DataTypeBinaryLargeObject
  | DataTypeCharacterLargeObject

  | DataTypeArray DataType Int
  | DataTypeRow [ (Text, DataType) ]

  | DataTypeDomain Text
  | DataTypeBytea
  | DataTypeSmallSerial
  | DataTypeSerial
  | DataTypeBigSerial
  | DataTypeUUID
  deriving (Show, Eq)

instance Hashable DataType where
  hashWithSalt salt = undefined

instance Sql92DisplaySyntax DataType where
  displaySyntax = undefined

instance IsSql92DataTypeSyntax DataType where
  domainType = DataTypeDomain
  charType = DataTypeChar False
  varCharType = DataTypeChar True
  nationalCharType = DataTypeNationalChar False
  nationalVarCharType = DataTypeNationalChar True
  bitType = DataTypeBit False
  varBitType = DataTypeBit True
  numericType = DataTypeNumeric
  decimalType = DataTypeDecimal
  intType = DataTypeInteger
  smallIntType = DataTypeSmallInt
  floatType = DataTypeFloat
  doubleType = DataTypeDoublePrecision
  realType = DataTypeReal

  dateType = DataTypeDate
  timeType = DataTypeTime
  timestampType = DataTypeTimeStamp

instance IsSql99DataTypeSyntax DataType where
  characterLargeObjectType = DataTypeCharacterLargeObject
  binaryLargeObjectType = DataTypeCharacterLargeObject
  booleanType = DataTypeBoolean
  arrayType = DataTypeArray
  rowType = DataTypeRow

instance IsSql2008BigIntDataTypeSyntax DataType where
  bigIntType = DataTypeBigInt

instance Sql92SerializableDataTypeSyntax DataType where
  serializeDataType dt = undefined

data SetQuantifier
  = SetQuantifierAll | SetQuantifierDistinct
  deriving (Show, Eq)

instance IsSql92AggregationSetQuantifierSyntax SetQuantifier where
  setQuantifierDistinct = SetQuantifierDistinct
  setQuantifierAll = SetQuantifierAll

data Expression
  = ExpressionValue PgValue
  | ExpressionDefault
  | ExpressionRow [ Expression ]

  | ExpressionIn Expression [ Expression ]
  | ExpressionInSelect Expression Select

  | ExpressionIsNull Expression
  | ExpressionIsNotNull Expression
  | ExpressionIsTrue Expression
  | ExpressionIsNotTrue Expression
  | ExpressionIsFalse Expression
  | ExpressionIsNotFalse Expression
  | ExpressionIsUnknown Expression
  | ExpressionIsNotUnknown Expression

  | ExpressionCase [(Expression, Expression)] Expression
  | ExpressionCoalesce [Expression]
  | ExpressionNullIf Expression Expression

  | ExpressionFieldName FieldName

  | ExpressionBetween Expression Expression Expression
  | ExpressionBinOp Text Expression Expression
  | ExpressionCompOp Text (Maybe ComparatorQuantifier) Expression Expression
  | ExpressionUnOp Text Expression

  | ExpressionPosition Expression Expression
  | ExpressionCast Expression DataType
  | ExpressionExtract ExtractField Expression
  | ExpressionCharLength Expression
  | ExpressionOctetLength Expression
  | ExpressionBitLength Expression
  | ExpressionAbs Expression
  | ExpressionLower Expression
  | ExpressionUpper Expression
  | ExpressionTrim Expression

  | ExpressionNamedFunction Text
  | ExpressionFunctionCall Expression [ Expression ]
  | ExpressionInstanceField Expression Text
  | ExpressionRefField Expression Text

  | ExpressionCountAll
  | ExpressionAgg Text (Maybe SetQuantifier) [ Expression ]
  | ExpressionBuiltinFunction Text [ Expression ]

  | ExpressionSubquery Select
  | ExpressionUnique Select
  | ExpressionDistinct Select
  | ExpressionExists Select

  | ExpressionOver Expression WindowFrame

  | ExpressionCurrentTimestamp
  deriving (Show, Eq)

instance IsSql92ExtractFieldSyntax ExtractField where
  secondsField = ExtractFieldDateTimeSecond
  minutesField = ExtractFieldDateTimeMinute
  hourField = ExtractFieldDateTimeHour
  dayField = ExtractFieldDateTimeDay
  monthField = ExtractFieldDateTimeMonth
  yearField = ExtractFieldDateTimeYear

instance IsSql92ExpressionSyntax Expression where
  type Sql92ExpressionQuantifierSyntax Expression = ComparatorQuantifier
  type Sql92ExpressionValueSyntax Expression = PgValue
  type Sql92ExpressionSelectSyntax Expression = Select
  type Sql92ExpressionFieldNameSyntax Expression = FieldName
  type Sql92ExpressionCastTargetSyntax Expression = DataType
  type Sql92ExpressionExtractFieldSyntax Expression = ExtractField

  valueE = ExpressionValue
  rowE = ExpressionRow

  isNullE = ExpressionIsNull
  isNotNullE = ExpressionIsNotNull
  isTrueE = ExpressionIsTrue
  isNotTrueE = ExpressionIsNotTrue
  isFalseE = ExpressionIsFalse
  isNotFalseE = ExpressionIsNotFalse
  isUnknownE = ExpressionIsUnknown
  isNotUnknownE = ExpressionIsNotUnknown

  caseE = ExpressionCase
  coalesceE = ExpressionCoalesce
  nullIfE = ExpressionNullIf
  positionE = ExpressionPosition
  extractE = ExpressionExtract
  castE = ExpressionCast

  fieldE = ExpressionFieldName

  betweenE = ExpressionBetween
  andE = ExpressionBinOp "AND"
  orE = ExpressionBinOp "OR"

  eqE = ExpressionCompOp "=="
  neqE = ExpressionCompOp "<>"
  ltE = ExpressionCompOp "<"
  gtE = ExpressionCompOp ">"
  leE = ExpressionCompOp "<="
  geE = ExpressionCompOp ">="
  addE = ExpressionBinOp "+"
  subE = ExpressionBinOp "-"
  mulE = ExpressionBinOp "*"
  divE = ExpressionBinOp "/"
  modE = ExpressionBinOp "%"
  likeE = ExpressionBinOp "LIKE"
  overlapsE = ExpressionBinOp "OVERLAPS"

  notE = ExpressionUnOp "NOT"
  negateE = ExpressionUnOp "-"

  charLengthE = ExpressionCharLength
  octetLengthE = ExpressionOctetLength
  bitLengthE = ExpressionBitLength
  absE = ExpressionAbs
  lowerE = ExpressionLower
  upperE = ExpressionUpper
  trimE = ExpressionTrim

  subqueryE = ExpressionSubquery
  uniqueE = ExpressionUnique
  existsE = ExpressionExists

  currentTimestampE = ExpressionCurrentTimestamp

  defaultE = ExpressionDefault
  inE = ExpressionIn
  inSelectE = ExpressionInSelect

instance IsSql99FunctionExpressionSyntax Expression where
  functionNameE = ExpressionNamedFunction
  functionCallE = ExpressionFunctionCall

instance IsSql99ExpressionSyntax Expression where
  distinctE = ExpressionDistinct
  similarToE = ExpressionBinOp "SIMILAR TO"
  instanceFieldE = ExpressionInstanceField
  refFieldE = ExpressionRefField

instance IsSql92AggregationExpressionSyntax Expression where
  type Sql92AggregationSetQuantifierSyntax Expression = SetQuantifier

  countAllE = ExpressionCountAll
  countE q = ExpressionAgg "COUNT" q . pure
  sumE q   = ExpressionAgg "SUM" q . pure
  minE q   = ExpressionAgg "MIN" q . pure
  maxE q   = ExpressionAgg "MAX" q . pure
  avgE q   = ExpressionAgg "AVG" q . pure

instance IsSql99AggregationExpressionSyntax Expression where
  everyE q = ExpressionAgg "EVERY" q . pure
  someE q  = ExpressionAgg "SOME" q . pure
  anyE q   = ExpressionAgg "ANY" q . pure

instance IsSql2003EnhancedNumericFunctionsExpressionSyntax Expression where
  lnE    = ExpressionBuiltinFunction "LN" . pure
  expE   = ExpressionBuiltinFunction "EXP" . pure
  sqrtE  = ExpressionBuiltinFunction "SQRT" . pure
  ceilE  = ExpressionBuiltinFunction "CEIL" . pure
  floorE = ExpressionBuiltinFunction "FLOOR" . pure
  powerE a b = ExpressionBuiltinFunction "POWER" [a, b]

instance IsSql2003EnhancedNumericFunctionsAggregationExpressionSyntax Expression where
  stddevPopE q  = ExpressionAgg "STDDEV_POP" q . pure
  stddevSampE q = ExpressionAgg "STDDEV_SAMP" q . pure
  varPopE q     = ExpressionAgg "VAR_POP" q . pure
  varSampE q    = ExpressionAgg "VAR_SAMP" q . pure

  covarPopE q a b      = ExpressionAgg "COVAR_POP" q [a, b]
  covarSampE q a b     = ExpressionAgg "COVAR_SAMP" q [a, b]
  corrE q a b          = ExpressionAgg "CORR" q [a, b]
  regrSlopeE q a b     = ExpressionAgg "REGR_SLOPE" q [a, b]
  regrInterceptE q a b = ExpressionAgg "REGR_INTERCEPT" q [a, b]
  regrCountE q a b     = ExpressionAgg "REGR_COUNT" q [a, b]
  regrRSquaredE q a b  = ExpressionAgg "REGR_R2" q [a, b]
  regrAvgXE q a b      = ExpressionAgg "REGR_AVGX" q [a, b]
  regrAvgYE q a b      = ExpressionAgg "REGR_AVGY" q [a, b]
  regrSXXE q a b       = ExpressionAgg "REGR_SXX" q [a, b]
  regrSXYE q a b       = ExpressionAgg "REGR_SXY" q [a, b]
  regrSYYE q a b       = ExpressionAgg "REGR_SYY" q [a, b]

instance IsSql2003NtileExpressionSyntax Expression where
  ntileE = ExpressionAgg "NTILE" Nothing . pure

instance IsSql2003LeadAndLagExpressionSyntax Expression where
  leadE x Nothing Nothing   = ExpressionAgg "LEAD" Nothing [x]
  leadE x (Just y) Nothing  = ExpressionAgg "LEAD" Nothing [x, y]
  leadE x (Just y) (Just z) = ExpressionAgg "LEAD" Nothing [x, y, z]
  leadE x Nothing (Just z)  = ExpressionAgg "LEAD" Nothing [x, ExpressionValue (PgValue (1 :: Int)), z]

  lagE x Nothing Nothing  = ExpressionAgg "LAG" Nothing [x]
  lagE x (Just y) Nothing = ExpressionAgg "LAG" Nothing [x, y]
  lagE x (Just y) (Just z) = ExpressionAgg "LAG" Nothing [x, y, z]
  lagE x Nothing (Just z)  = ExpressionAgg "LAG" Nothing [x, ExpressionValue (PgValue (1 :: Int)), z]

instance IsSql2003NthValueExpressionSyntax Expression where
  nthValueE a b = ExpressionAgg "NTH_VALUE" Nothing [a, b]

instance IsSql2003ExpressionSyntax Expression where
  type Sql2003ExpressionWindowFrameSyntax Expression = WindowFrame

  overE = ExpressionOver
  rowNumberE = ExpressionAgg "ROW_NUMBER" Nothing []

newtype Projection
  = ProjExprs [ (Expression, Maybe Text ) ]
  deriving (Show, Eq)

instance IsSql92ProjectionSyntax Projection where
  type Sql92ProjectionExpressionSyntax Projection = Expression

  projExprs = ProjExprs

data Ordering
  = OrderingAsc Expression
  | OrderingDesc Expression
  deriving (Show, Eq)

instance IsSql92OrderingSyntax Ordering where
  type Sql92OrderingExpressionSyntax Ordering = Expression

  ascOrdering = OrderingAsc
  descOrdering = OrderingDesc

newtype Grouping = Grouping [ Expression ] deriving (Show, Eq)

instance IsSql92GroupingSyntax Grouping where
  type Sql92GroupingExpressionSyntax Grouping = Expression

  groupByExpressions = Grouping

data TableName = TableName (Maybe Text) Text
  deriving (Show, Eq, Ord)

instance IsSql92TableNameSyntax TableName where
  tableName = TableName

data TableSource
  = TableNamed TableName
  | TableFromSubSelect Select
  | TableFromValues [ [ Expression ] ]
  deriving (Show, Eq)

instance IsSql92TableSourceSyntax TableSource where
  type Sql92TableSourceSelectSyntax TableSource = Select
  type Sql92TableSourceExpressionSyntax TableSource = Expression
  type Sql92TableSourceTableNameSyntax TableSource = TableName

  tableNamed = TableNamed
  tableFromSubSelect = TableFromSubSelect
  tableFromValues = TableFromValues

data From
  = FromTable TableSource (Maybe (Text, Maybe [Text]))
  | InnerJoin From From (Maybe Expression)
  | LeftJoin From From (Maybe Expression)
  | RightJoin From From (Maybe Expression)
  | OuterJoin From From (Maybe Expression)
  deriving (Show, Eq)

instance IsSql92FromSyntax From where
  type Sql92FromTableSourceSyntax From = TableSource
  type Sql92FromExpressionSyntax From = Expression

  fromTable = FromTable
  innerJoin = InnerJoin
  leftJoin = LeftJoin
  rightJoin = RightJoin

data PgValue where
  PgValue :: (Show a, Eq a, Typeable a) => a -> PgValue

#define VALUE_SYNTAX_INSTANCE(ty) instance HasSqlValueSyntax PgValue ty where { sqlValueSyntax = PgValue }
VALUE_SYNTAX_INSTANCE(Int16)
VALUE_SYNTAX_INSTANCE(Int32)
VALUE_SYNTAX_INSTANCE(Int64)
VALUE_SYNTAX_INSTANCE(Word16)
VALUE_SYNTAX_INSTANCE(Word32)
VALUE_SYNTAX_INSTANCE(Word64)
VALUE_SYNTAX_INSTANCE(Integer)
VALUE_SYNTAX_INSTANCE(String)
VALUE_SYNTAX_INSTANCE(Text)
VALUE_SYNTAX_INSTANCE(ByteString)
VALUE_SYNTAX_INSTANCE(LocalTime)
VALUE_SYNTAX_INSTANCE(UTCTime)
VALUE_SYNTAX_INSTANCE(Day)
VALUE_SYNTAX_INSTANCE(TimeOfDay)
VALUE_SYNTAX_INSTANCE(SqlNull)
VALUE_SYNTAX_INSTANCE(Double)
VALUE_SYNTAX_INSTANCE(Bool)

instance TypeError (PreferExplicitSize Int Int32) => HasSqlValueSyntax PgValue Int where
  sqlValueSyntax = PgValue

instance TypeError (PreferExplicitSize Word Word32) => HasSqlValueSyntax PgValue Word where
  sqlValueSyntax = PgValue

instance HasSqlValueSyntax PgValue x => HasSqlValueSyntax PgValue (Maybe x) where
  sqlValueSyntax (Just x) = sqlValueSyntax x
  sqlValueSyntax Nothing = sqlValueSyntax SqlNull

instance Eq PgValue where
  PgValue a == PgValue b =
    case cast a of
      Just a' -> a' == b
      Nothing -> False
instance Show PgValue where
  showsPrec prec (PgValue a) =
    showParen (prec > app_prec) $
    ("PgValue " ++ ).
    showsPrec (app_prec + 1) a
    where app_prec = 10

-- Window functions

data WindowFrame
  = WindowFrame
  { windowFramePartitions :: Maybe [Expression]
  , windowFrameOrdering   ::  Maybe [Ordering]
  , windowFrameBounds     :: Maybe WindowFrameBounds
  } deriving (Show, Eq)

instance IsSql2003WindowFrameSyntax WindowFrame where
  type Sql2003WindowFrameExpressionSyntax WindowFrame = Expression
  type Sql2003WindowFrameOrderingSyntax WindowFrame = Ordering
  type Sql2003WindowFrameBoundsSyntax WindowFrame = WindowFrameBounds

  frameSyntax = WindowFrame

data WindowFrameBounds
  = WindowFrameBounds
  { boundsFrom :: WindowFrameBound
  , boundsTo   :: Maybe WindowFrameBound
  } deriving (Show, Eq)

instance IsSql2003WindowFrameBoundsSyntax WindowFrameBounds where
  type Sql2003WindowFrameBoundsBoundSyntax WindowFrameBounds = WindowFrameBound

  fromToBoundSyntax = WindowFrameBounds

data WindowFrameBound
  = WindowFrameUnbounded
  | WindowFrameBoundNRows Int
  deriving (Show, Eq)

instance IsSql2003WindowFrameBoundSyntax WindowFrameBound where
  unboundedSyntax = WindowFrameUnbounded
  nrowsBoundSyntax = WindowFrameBoundNRows

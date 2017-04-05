{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.Beam.Postgres.Syntax
    ( PgSyntaxF(..), PgSyntaxM
    , PgSyntax(..)

    , emit, emitBuilder, escapeString
    , escapeBytea, escapeIdentifier

    , nextSyntaxStep

    , PgCommandSyntax(..)
    , PgSelectSyntax(..)
    , PgInsertSyntax(..)
    , PgDeleteSyntax(..)
    , PgUpdateSyntax(..)

    , PgExpressionSyntax(..), PgFromSyntax(..)
    , PgComparisonQuantifierSyntax(..)
    , PgCastTargetSyntax(..), PgExtractFieldSyntax(..)
    , PgProjectionSyntax(..), PgGroupingSyntax(..)
    , PgOrderingSyntax(..), PgValueSyntax(..)
    , PgTableSourceSyntax(..), PgFieldNameSyntax(..)
    , PgInsertValuesSyntax(..), PgInsertOnConflictSyntax(..)
    , PgInsertOnConflictTargetSyntax(..), PgInsertOnConflictUpdateSyntax(..)
    , PgCreateTableSyntax(..), PgTableOptionsSyntax(..), PgColumnSchemaSyntax(..)
    , PgDataTypeSyntax(..), PgColumnConstraintDefinitionSyntax(..), PgColumnConstraintSyntax(..)
    , PgTableConstraintSyntax(..), PgMatchTypeSyntax(..), PgReferentialActionSyntax(..)

    , PgWindowFrameSyntax(..), PgWindowFrameBoundsSyntax(..), PgWindowFrameBoundSyntax(..)

    , insertDefaults
    , pgSimpleMatchSyntax
    , pgBooleanType, pgByteaType

    , IsPgInsertOnConflictSyntax(..)
    , PgInsertOnConflict(..)
    , onConflictDefault, onConflictUpdate, onConflictDoNothing

    , pgQuotedIdentifier, pgUnOp, pgSepBy, pgDebugRenderSyntax
    , pgBuildAction

    , insert

    , PgInsertReturning(..)
    , insertReturning

    , now_ ) where

import           Database.Beam.Postgres.Types

import           Database.Beam hiding (insert)
import           Database.Beam.Backend.SQL
import           Database.Beam.Backend.Types
import           Database.Beam.Migrate.SQL
import           Database.Beam.Migrate.SQL.Builder hiding (fromSqlConstraintAttributes)
--import           Database.Beam.Query.Combinators
import           Database.Beam.Query.Internal
--import           Database.Beam.Schema.Tables

import           Control.Monad.Free
import           Control.Monad.Free.Church
import           Control.Monad.State

import           Data.ByteString (ByteString)
import           Data.ByteString.Builder (Builder, toLazyByteString)
import           Data.ByteString.Lazy (toStrict)
import           Data.Coerce
import           Data.Monoid
import           Data.Proxy
import           Data.Ratio
import           Data.String (fromString)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import           Data.Time (LocalTime)

import qualified Database.PostgreSQL.Simple as Pg
import qualified Database.PostgreSQL.Simple.ToField as Pg

data PostgresInaccessible

data PgSyntaxF f where
  EmitByteString :: ByteString -> f -> PgSyntaxF f
  EmitBuilder    :: Builder -> f -> PgSyntaxF f

  EscapeString :: ByteString -> f -> PgSyntaxF f
  EscapeBytea  :: ByteString -> f -> PgSyntaxF f
  EscapeIdentifier :: ByteString -> f -> PgSyntaxF f
deriving instance Functor PgSyntaxF

instance Eq f => Eq (PgSyntaxF f) where
  EmitByteString b1 next1 == EmitByteString b2 next2 =
      b1 == b2 && next1 == next2
  EmitBuilder b1 next1 == EmitBuilder b2 next2 =
      toLazyByteString b1 == toLazyByteString b2 && next1 == next2
  EscapeString b1 next1 == EscapeString b2 next2 =
      b1 == b2 && next1 == next2
  EscapeBytea b1 next1 == EscapeBytea b2 next2 =
      b1 == b2 && next1 == next2
  EscapeIdentifier b1 next1 == EscapeIdentifier b2 next2 =
      b1 == b2 && next1 == next2
  _ == _ = False

type PgSyntaxM = F PgSyntaxF
newtype PgSyntax
  = PgSyntax { buildPgSyntax :: PgSyntaxM () }

instance Monoid PgSyntax where
  mempty = PgSyntax (pure ())
  mappend a b = PgSyntax (buildPgSyntax a >> buildPgSyntax b)

instance Eq PgSyntax where
  PgSyntax x == PgSyntax y = (fromF x :: Free PgSyntaxF ()) == fromF y

emit :: ByteString -> PgSyntax
emit bs = PgSyntax (liftF (EmitByteString bs ()))

emitBuilder :: Builder -> PgSyntax
emitBuilder b = PgSyntax (liftF (EmitBuilder b ()))

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

-- * Syntax types

newtype PgCommandSyntax = PgCommandSyntax { fromPgCommand :: PgSyntax }
instance SupportedSyntax Postgres PgCommandSyntax

newtype PgSelectSyntax = PgSelectSyntax { fromPgSelect :: PgSyntax }
instance SupportedSyntax Postgres PgSelectSyntax

newtype PgSelectTableSyntax = PgSelectTableSyntax { fromPgSelectTable :: PgSyntax }
instance SupportedSyntax Postgres PgSelectTableSyntax

newtype PgInsertSyntax = PgInsertSyntax { fromPgInsert :: PgSyntax }
instance SupportedSyntax Postgres PgInsertSyntax

newtype PgDeleteSyntax = PgDeleteSyntax { fromPgDelete :: PgSyntax }
instance SupportedSyntax Postgres PgDeleteSyntax

newtype PgUpdateSyntax = PgUpdateSyntax { fromPgUpdate :: PgSyntax }
instance SupportedSyntax Postgres PgUpdateSyntax

newtype PgExpressionSyntax = PgExpressionSyntax { fromPgExpression :: PgSyntax } deriving Eq
newtype PgAggregationSetQuantifierSyntax = PgAggregationSetQuantifierSyntax { fromPgAggregationSetQuantifier :: PgSyntax }
newtype PgFromSyntax = PgFromSyntax { fromPgFrom :: PgSyntax }
newtype PgComparisonQuantifierSyntax = PgComparisonQuantifierSyntax { fromPgComparisonQuantifier :: PgSyntax }
newtype PgCastTargetSyntax = PgCastTargetSyntax { fromPgCastTarget :: PgSyntax }
newtype PgExtractFieldSyntax = PgExtractFieldSyntax { fromPgExtractField :: PgSyntax }
newtype PgProjectionSyntax = PgProjectionSyntax { fromPgProjection :: PgSyntax }
newtype PgGroupingSyntax = PgGroupingSyntax { fromPgGrouping :: PgSyntax }
newtype PgOrderingSyntax = PgOrderingSyntax { fromPgOrdering :: PgSyntax }
newtype PgValueSyntax = PgValueSyntax { fromPgValue :: PgSyntax }
newtype PgTableSourceSyntax = PgTableSourceSyntax { fromPgTableSource :: PgSyntax }
newtype PgFieldNameSyntax = PgFieldNameSyntax { fromPgFieldName :: PgSyntax }
newtype PgInsertValuesSyntax = PgInsertValuesSyntax { fromPgInsertValues :: PgSyntax }
newtype PgInsertOnConflictSyntax = PgInsertOnConflictSyntax { fromPgInsertOnConflict :: PgSyntax }
newtype PgInsertOnConflictTargetSyntax = PgInsertOnConflictTargetSyntax { fromPgInsertOnConflictTarget :: PgSyntax }
newtype PgInsertOnConflictUpdateSyntax = PgInsertOnConflictUpdateSyntax { fromPgInsertOnConflictUpdate :: PgSyntax }

newtype PgCreateTableSyntax = PgCreateTableSyntax { fromPgCreateTable :: PgSyntax }
data PgTableOptionsSyntax = PgTableOptionsSyntax PgSyntax PgSyntax
newtype PgColumnSchemaSyntax = PgColumnSchemaSyntax { fromPgColumnSchema :: PgSyntax }
newtype PgDataTypeSyntax = PgDataTypeSyntax { fromPgDataType :: PgSyntax }
newtype PgColumnConstraintDefinitionSyntax = PgColumnConstraintDefinitionSyntax { fromPgColumnConstraintDefinition :: PgSyntax }
newtype PgColumnConstraintSyntax = PgColumnConstraintSyntax { fromPgColumnConstraint :: PgSyntax }
newtype PgTableConstraintSyntax = PgTableConstraintSyntax { fromPgTableConstraint :: PgSyntax }
newtype PgMatchTypeSyntax = PgMatchTypeSyntax { fromPgMatchType :: PgSyntax }
newtype PgReferentialActionSyntax = PgReferentialActionSyntax { fromPgReferentialAction :: PgSyntax }
newtype PgWindowFrameSyntax = PgWindowFrameSyntax { fromPgWindowFrame :: PgSyntax }
newtype PgWindowFrameBoundsSyntax = PgWindowFrameBoundsSyntax { fromPgWindowFrameBounds :: PgSyntax }
newtype PgWindowFrameBoundSyntax = PgWindowFrameBoundSyntax { fromPgWindowFrameBound :: ByteString -> PgSyntax }

instance IsSql92Syntax PgCommandSyntax where
  type Sql92SelectSyntax PgCommandSyntax = PgSelectSyntax
  type Sql92InsertSyntax PgCommandSyntax = PgInsertSyntax
  type Sql92UpdateSyntax PgCommandSyntax = PgUpdateSyntax
  type Sql92DeleteSyntax PgCommandSyntax = PgDeleteSyntax

  selectCmd = coerce
  insertCmd = coerce
  deleteCmd = coerce
  updateCmd = coerce

instance IsSql92DdlCommandSyntax PgCommandSyntax where
  type Sql92DdlCommandCreateTableSyntax PgCommandSyntax = PgCreateTableSyntax

  createTableCmd = coerce

instance IsSql92UpdateSyntax PgUpdateSyntax where
  type Sql92UpdateFieldNameSyntax PgUpdateSyntax = PgFieldNameSyntax
  type Sql92UpdateExpressionSyntax PgUpdateSyntax = PgExpressionSyntax

  updateStmt tbl fields where_ =
    PgUpdateSyntax $
    emit "UPDATE " <> pgQuotedIdentifier tbl <>
    (case fields of
       [] -> mempty
       fields ->
         emit " SET " <>
         pgSepBy (emit ", ") (map (\(field, val) -> fromPgFieldName field <> emit "=" <> fromPgExpression val) fields)) <>
    maybe mempty (\where_ -> emit " WHERE " <> fromPgExpression where_) where_

instance IsSql92DeleteSyntax PgDeleteSyntax where
  type Sql92DeleteExpressionSyntax PgDeleteSyntax = PgExpressionSyntax

  deleteStmt tbl where_ =
    PgDeleteSyntax $
    emit "DELETE FROM " <> pgQuotedIdentifier tbl <>
    maybe mempty (\where_ -> emit " WHERE " <> fromPgExpression where_) where_

instance IsSql92SelectSyntax PgSelectSyntax where
  type Sql92SelectSelectTableSyntax PgSelectSyntax = PgSelectTableSyntax
  type Sql92SelectOrderingSyntax PgSelectSyntax = PgOrderingSyntax

  selectStmt tbl ordering limit offset =
    PgSelectSyntax $
    coerce tbl <>
    (case ordering of
       [] -> mempty
       ordering -> emit " ORDER BY " <> pgSepBy (emit ", ") (coerce ordering)) <>
    (maybe mempty (emit . fromString . (" LIMIT " <>) . show) limit) <>
    (maybe mempty (emit . fromString . (" OFFSET " <>) . show) offset)

instance IsSql92SelectTableSyntax PgSelectTableSyntax where
  type Sql92SelectTableSelectSyntax PgSelectTableSyntax = PgSelectSyntax
  type Sql92SelectTableExpressionSyntax PgSelectTableSyntax = PgExpressionSyntax
  type Sql92SelectTableProjectionSyntax PgSelectTableSyntax = PgProjectionSyntax
  type Sql92SelectTableFromSyntax PgSelectTableSyntax = PgFromSyntax
  type Sql92SelectTableGroupingSyntax PgSelectTableSyntax = PgGroupingSyntax

  selectTableStmt proj from where_ grouping having =
    PgSelectTableSyntax $
    emit "SELECT " <> fromPgProjection proj <>
    (maybe mempty (emit " FROM " <> ) (coerce from)) <>
    (maybe mempty (emit " WHERE " <>) (coerce where_)) <>
    (maybe mempty (emit " GROUP BY " <>) (coerce grouping)) <>
    (maybe mempty (emit " HAVING " <>) (coerce having))

  unionTables all = pgTableOp (if all then "UNION ALL" else "UNION")
  intersectTables all = pgTableOp (if all then "INTERSECT ALL" else "INTERSECT")
  exceptTable all = pgTableOp (if all then "EXCEPT ALL" else "EXCEPT")

instance IsSql92GroupingSyntax PgGroupingSyntax where
  type Sql92GroupingExpressionSyntax PgGroupingSyntax = PgExpressionSyntax

  groupByExpressions es =
      PgGroupingSyntax $
      pgSepBy (emit ", ") (map fromPgExpression es)

instance IsSql92FromSyntax PgFromSyntax where
  type Sql92FromExpressionSyntax PgFromSyntax = PgExpressionSyntax
  type Sql92FromTableSourceSyntax PgFromSyntax = PgTableSourceSyntax

  fromTable tableSrc Nothing = coerce tableSrc
  fromTable tableSrc (Just nm) =
      PgFromSyntax $
      coerce tableSrc <> emit " AS " <> pgQuotedIdentifier nm

  innerJoin = pgJoin "INNER JOIN"
  leftJoin = pgJoin "LEFT JOIN"
  rightJoin = pgJoin "RIGHT JOIN"

instance IsSql92OrderingSyntax PgOrderingSyntax where
  type Sql92OrderingExpressionSyntax PgOrderingSyntax = PgExpressionSyntax

  ascOrdering e = PgOrderingSyntax (fromPgExpression e <> emit " ASC")
  descOrdering e = PgOrderingSyntax (fromPgExpression e <> emit " DESC")

instance IsSql92DataTypeSyntax PgDataTypeSyntax where
  domainType nm = PgDataTypeSyntax (pgQuotedIdentifier nm)

  charType prec charSet = PgDataTypeSyntax (emit "CHAR" <> pgOptPrec prec <> pgOptCharSet charSet)
  varCharType prec charSet = PgDataTypeSyntax (emit "VARCHAR" <> pgOptPrec prec <> pgOptCharSet charSet)
  nationalCharType prec = PgDataTypeSyntax (emit "NATIONAL CHAR" <> pgOptPrec prec)
  nationalVarCharType prec = PgDataTypeSyntax (emit "NATIONAL CHARACTER VARYING" <> pgOptPrec prec)

  bitType prec = PgDataTypeSyntax (emit "BIT" <> pgOptPrec prec)
  varBitType prec = PgDataTypeSyntax (emit "BIT VARYING" <> pgOptPrec prec)

  numericType prec = PgDataTypeSyntax (emit "NUMERIC" <> pgOptNumericPrec prec)
  decimalType prec = PgDataTypeSyntax (emit "DOUBLE" <> pgOptNumericPrec prec)

  intType = PgDataTypeSyntax (emit "INT")
  smallIntType = PgDataTypeSyntax (emit "SMALLINT")

  floatType prec = PgDataTypeSyntax (emit "FLOAT" <> pgOptPrec prec)
  doubleType = PgDataTypeSyntax (emit "DOUBLE PRECISION")
  realType = PgDataTypeSyntax (emit "REAL")
  dateType = PgDataTypeSyntax (emit "DATE")
  timeType prec withTz = PgDataTypeSyntax (emit "TIME" <> pgOptPrec prec <> if withTz then emit " WITH TIME ZONE" else mempty)
  timestampType prec withTz = PgDataTypeSyntax (emit "TIMESTAMP" <> pgOptPrec prec <> if withTz then emit " WITH TIME ZONE" else mempty)

pgOptPrec :: Maybe Word -> PgSyntax
pgOptPrec Nothing = mempty
pgOptPrec (Just x) = emit "(" <> emit (fromString (show x)) <> emit ")"

pgOptCharSet :: Maybe T.Text -> PgSyntax
pgOptCharSet Nothing = mempty
pgOptCharSet (Just cs) = emit " CHARACTER SET " <> emit (TE.encodeUtf8 cs)

pgOptNumericPrec :: Maybe (Word, Maybe Word) -> PgSyntax
pgOptNumericPrec Nothing = mempty
pgOptNumericPrec (Just (prec, Nothing)) = pgOptPrec (Just prec)
pgOptNumericPrec (Just (prec, Just dec)) = emit "(" <> emit (fromString (show prec)) <> emit ", " <> emit (fromString (show dec)) <> emit ")"

pgBooleanType :: PgDataTypeSyntax
pgBooleanType = PgDataTypeSyntax (emit "BOOLEAN")

pgByteaType :: PgDataTypeSyntax
pgByteaType = PgDataTypeSyntax (emit "BYTEA")

instance IsSql92ExpressionSyntax PgExpressionSyntax where
  type Sql92ExpressionValueSyntax PgExpressionSyntax = PgValueSyntax
  type Sql92ExpressionSelectSyntax PgExpressionSyntax = PgSelectSyntax
  type Sql92ExpressionFieldNameSyntax PgExpressionSyntax = PgFieldNameSyntax
  type Sql92ExpressionQuantifierSyntax PgExpressionSyntax = PgComparisonQuantifierSyntax
  type Sql92ExpressionCastTargetSyntax PgExpressionSyntax = PgCastTargetSyntax
  type Sql92ExpressionExtractFieldSyntax PgExpressionSyntax = PgExtractFieldSyntax

  addE = pgBinOp "+"
  subE = pgBinOp "-"
  mulE = pgBinOp "*"
  divE = pgBinOp "/"
  modE = pgBinOp "%"
  orE = pgBinOp "OR"
  andE = pgBinOp "AND"
  likeE = pgBinOp "LIKE"
  overlapsE = pgBinOp "OVERLAPS"
  eqE = pgCompOp "="
  neqE = pgCompOp "<>"
  ltE = pgCompOp "<"
  gtE = pgCompOp ">"
  leE = pgCompOp "<="
  geE = pgCompOp ">="
  negateE = pgUnOp "-"
  notE = pgUnOp "NOT"
  existsE select = PgExpressionSyntax (emit "EXISTS (" <> fromPgSelect select <> emit ")")
  uniqueE select = PgExpressionSyntax (emit "UNIQUE (" <> fromPgSelect select <> emit ")")
  isNotNullE = pgPostFix "IS NOT NULL"
  isNullE = pgPostFix "IS NULL"
  isTrueE = pgPostFix "IS TRUE"
  isFalseE = pgPostFix "IS FALSE"
  isNotTrueE = pgPostFix "IS NOT TRUE"
  isNotFalseE = pgPostFix "IS NOT FALSE"
  isUnknownE = pgPostFix "IS UNKNOWN"
  isNotUnknownE = pgPostFix "IS NOT UNKNOWN"
  betweenE a b c = PgExpressionSyntax (emit "(" <> fromPgExpression a <> emit ") BETWEEN (" <>
                                       fromPgExpression b <> emit ") AND (" <> fromPgExpression c <> emit ")")
  valueE = coerce
  rowE vs = PgExpressionSyntax $
            emit "(" <>
            pgSepBy (emit ", ") (coerce vs) <>
            emit ")"
  fieldE = coerce
  subqueryE s = PgExpressionSyntax (emit "(" <> fromPgSelect s <> emit ")")
  positionE needle haystack =
      PgExpressionSyntax $
      emit "POSITION((" <> fromPgExpression needle <> emit ") IN (" <> fromPgExpression haystack <> emit "))"
  nullIfE a b = PgExpressionSyntax (emit "NULLIF(" <> fromPgExpression a <> emit ", " <> fromPgExpression b <> emit ")")
  absE x = PgExpressionSyntax (emit "ABS(" <> fromPgExpression x <> emit ")")
  bitLengthE x = PgExpressionSyntax (emit "BIT_LENGTH(" <> fromPgExpression x <> emit ")")
  charLengthE x = PgExpressionSyntax (emit "CHAR_LENGTH(" <> fromPgExpression x <> emit ")")
  octetLengthE x = PgExpressionSyntax (emit "OCTET_LENGTH(" <> fromPgExpression x <> emit ")")
  coalesceE es = PgExpressionSyntax (emit "COALESCE(" <> pgSepBy (emit ", ") (map fromPgExpression es) <> emit ")")
  extractE field from = PgExpressionSyntax (emit "EXTRACT(" <> fromPgExtractField field <> emit " FROM (" <> fromPgExpression from <> emit "))")
  castE e to = PgExpressionSyntax (emit "CAST((" <> fromPgExpression e <> emit ") TO " <> fromPgCastTarget to <> emit ")")
  caseE cases else_ =
      PgExpressionSyntax $
      emit "CASE " <>
      foldMap (\(cond, res) -> emit "WHEN " <> fromPgExpression cond <> emit " THEN " <> fromPgExpression res <> emit " ") cases <>
      emit "ELSE " <> fromPgExpression else_ <> emit " END"

instance IsSql99ExpressionSyntax PgExpressionSyntax where
  distinctE select = PgExpressionSyntax (emit "DISTINCT (" <> fromPgSelect select <> emit ")")
  similarToE = pgBinOp "SIMILAR TO"

instance IsSql2003ExpressionSyntax PgExpressionSyntax where
  type Sql2003ExpressionWindowFrameSyntax PgExpressionSyntax =
    PgWindowFrameSyntax

  overE expr frame =
    PgExpressionSyntax $
    fromPgExpression expr <> emit " " <> fromPgWindowFrame frame

instance IsSql2003WindowFrameSyntax PgWindowFrameSyntax where
  type Sql2003WindowFrameExpressionSyntax PgWindowFrameSyntax = PgExpressionSyntax
  type Sql2003WindowFrameOrderingSyntax PgWindowFrameSyntax = PgOrderingSyntax
  type Sql2003WindowFrameBoundsSyntax PgWindowFrameSyntax = PgWindowFrameBoundsSyntax

  frameSyntax filter_ partition_ ordering_ bounds_ =
    PgWindowFrameSyntax $
    maybe mempty (\e -> emit "FILTER (WHERE" <> fromPgExpression e <> emit ")") filter_ <>
    emit "OVER " <>
    pgParens
    (
      maybe mempty (\p -> emit "PARTITION BY " <> pgSepBy (emit ", ") (map fromPgExpression p)) partition_ <>
      maybe mempty (\o -> emit " ORDER BY " <> pgSepBy (emit ", ") (map fromPgOrdering o)) ordering_ <>
      maybe mempty (\b -> emit " RANGE " <> fromPgWindowFrameBounds b) bounds_
    )

instance IsSql2003WindowFrameBoundsSyntax PgWindowFrameBoundsSyntax where
  type Sql2003WindowFrameBoundsBoundSyntax PgWindowFrameBoundsSyntax = PgWindowFrameBoundSyntax

  fromToBoundSyntax from Nothing =
    PgWindowFrameBoundsSyntax (fromPgWindowFrameBound from "PRECEDING")
  fromToBoundSyntax from (Just to) =
    PgWindowFrameBoundsSyntax $
    emit "BETWEEN " <> fromPgWindowFrameBound from "PRECEDING" <> emit " AND " <> fromPgWindowFrameBound to "FOLLOWING"

instance IsSql2003WindowFrameBoundSyntax PgWindowFrameBoundSyntax where
  unboundedSyntax = PgWindowFrameBoundSyntax $ \where_ -> emit "UNBOUNDED " <> emit where_
  nrowsBoundSyntax 0 = PgWindowFrameBoundSyntax $ \_ -> emit "CURRENT ROW"
  nrowsBoundSyntax n = PgWindowFrameBoundSyntax $ \where_ -> emit (fromString (show n)) <> emit " " <> emit where_

instance IsSql92AggregationExpressionSyntax PgExpressionSyntax where
  type Sql92AggregationSetQuantifierSyntax PgExpressionSyntax = PgAggregationSetQuantifierSyntax

  countAllE = PgExpressionSyntax (emit "COUNT(*)")
  countE = pgUnAgg "COUNT"
  avgE = pgUnAgg "AVG"
  sumE = pgUnAgg "SUM"
  minE = pgUnAgg "MIN"
  maxE = pgUnAgg "MAX"

instance IsSql92AggregationSetQuantifierSyntax PgAggregationSetQuantifierSyntax where
  setQuantifierDistinct = PgAggregationSetQuantifierSyntax $ emit "DISTINCT"
  setQuantifierAll = PgAggregationSetQuantifierSyntax $ emit "ALL"

pgUnAgg :: ByteString -> Maybe PgAggregationSetQuantifierSyntax -> PgExpressionSyntax -> PgExpressionSyntax
pgUnAgg fn q e =
  PgExpressionSyntax $
  emit fn <> emit "(" <> maybe mempty (\q -> fromPgAggregationSetQuantifier q <> emit " ") q <> fromPgExpression e <> emit ")"

instance IsSql92FieldNameSyntax PgFieldNameSyntax where
  qualifiedField a b =
    PgFieldNameSyntax $
    pgQuotedIdentifier a <> emit "." <> pgQuotedIdentifier b
  unqualifiedField = PgFieldNameSyntax . pgQuotedIdentifier

instance IsSql92TableSourceSyntax PgTableSourceSyntax where
  type Sql92TableSourceSelectSyntax PgTableSourceSyntax = PgSelectSyntax
  tableNamed = PgTableSourceSyntax . pgQuotedIdentifier
  tableFromSubSelect s = PgTableSourceSyntax $ emit "(" <> fromPgSelect s <> emit ")"

instance IsSql92ProjectionSyntax PgProjectionSyntax where
  type Sql92ProjectionExpressionSyntax PgProjectionSyntax = PgExpressionSyntax

  projExprs exprs =
    PgProjectionSyntax $
    pgSepBy (emit ", ")
            (map (\(expr, nm) -> fromPgExpression expr <>
                                 maybe mempty (\nm -> emit " AS " <> pgQuotedIdentifier nm) nm) exprs)

instance IsSql92InsertSyntax PgInsertSyntax where
  type Sql92InsertValuesSyntax PgInsertSyntax = PgInsertValuesSyntax

  insertStmt tblName fields values =
      PgInsertSyntax $
      emit "INSERT INTO " <> pgQuotedIdentifier tblName <> emit "(" <>
      pgSepBy (emit ", ") (map pgQuotedIdentifier fields) <>
      emit ") " <> fromPgInsertValues values

instance IsSql92InsertValuesSyntax PgInsertValuesSyntax where
  type Sql92InsertValuesExpressionSyntax PgInsertValuesSyntax = PgExpressionSyntax
  type Sql92InsertValuesSelectSyntax PgInsertValuesSyntax = PgSelectSyntax

  insertSqlExpressions es =
      PgInsertValuesSyntax $
      emit "VALUES " <>
      pgSepBy (emit ", ")
              (map (\es -> emit "(" <> pgSepBy (emit ", ") (coerce es) <> emit ")")
                   es)
  insertFromSql (PgSelectSyntax a) = PgInsertValuesSyntax a

insertDefaults :: SqlInsertValues PgInsertValuesSyntax tbl
insertDefaults = SqlInsertValues (PgInsertValuesSyntax (emit "DEFAULT VALUES"))

instance IsSql92CreateTableSyntax PgCreateTableSyntax where
  type Sql92CreateTableColumnSchemaSyntax PgCreateTableSyntax = PgColumnSchemaSyntax
  type Sql92CreateTableTableConstraintSyntax PgCreateTableSyntax = PgTableConstraintSyntax
  type Sql92CreateTableOptionsSyntax PgCreateTableSyntax = PgTableOptionsSyntax

  createTableSyntax options tblNm fieldTypes constraints =
    let (beforeOptions, afterOptions) =
          case options of
            Nothing -> (emit " ", emit " ")
            Just (PgTableOptionsSyntax before after) ->
              ( emit " " <> before <> emit " "
              , emit " " <> after <> emit " " )
    in PgCreateTableSyntax $
       emit "CREATE" <> beforeOptions <> emit "TABLE " <> pgQuotedIdentifier tblNm <>
       emit " (" <>
       pgSepBy (emit ", ")
               (map (\(nm, type_) -> pgQuotedIdentifier nm <> emit " " <> fromPgColumnSchema type_)  fieldTypes <>
                map fromPgTableConstraint constraints)
       <> emit ")" <> afterOptions

instance IsSql92TableConstraintSyntax PgTableConstraintSyntax where
  primaryKeyConstraintSyntax fieldNames =
    PgTableConstraintSyntax $
    emit "PRIMARY KEY(" <> pgSepBy (emit ", ") (map pgQuotedIdentifier fieldNames) <> emit ")"

instance IsSql92ColumnSchemaSyntax PgColumnSchemaSyntax where
  type Sql92ColumnSchemaColumnTypeSyntax PgColumnSchemaSyntax = PgDataTypeSyntax
  type Sql92ColumnSchemaExpressionSyntax PgColumnSchemaSyntax = PgExpressionSyntax
  type Sql92ColumnSchemaColumnConstraintDefinitionSyntax PgColumnSchemaSyntax = PgColumnConstraintDefinitionSyntax

  columnSchemaSyntax colType defaultClause constraints collation =
    PgColumnSchemaSyntax $
    fromPgDataType colType <>
    maybe mempty (\d -> emit " DEFAULT " <> fromPgExpression d) defaultClause <>
    (case constraints of
       [] -> mempty
       _ -> foldMap (\c -> emit " " <> fromPgColumnConstraintDefinition c) constraints) <>
    maybe mempty (\nm -> emit " COLLATE " <> pgQuotedIdentifier nm) collation

instance IsSql92MatchTypeSyntax PgMatchTypeSyntax where
  fullMatchSyntax = PgMatchTypeSyntax (emit "FULL")
  partialMatchSyntax = PgMatchTypeSyntax (emit "PARTIAL")

pgSimpleMatchSyntax :: PgMatchTypeSyntax
pgSimpleMatchSyntax = PgMatchTypeSyntax (emit "SIMPLE")

instance IsSql92ReferentialActionSyntax PgReferentialActionSyntax where
  referentialActionCascadeSyntax = PgReferentialActionSyntax (emit "CASCADE")
  referentialActionNoActionSyntax = PgReferentialActionSyntax (emit "NO ACTION")
  referentialActionSetDefaultSyntax = PgReferentialActionSyntax (emit "SET DEFAULT")
  referentialActionSetNullSyntax = PgReferentialActionSyntax (emit "SET NULL")

fromSqlConstraintAttributes :: SqlConstraintAttributesBuilder -> PgSyntax
fromSqlConstraintAttributes (SqlConstraintAttributesBuilder timing deferrable) =
  maybe mempty timingBuilder timing <> maybe mempty deferrableBuilder deferrable
  where timingBuilder InitiallyDeferred = emit "INITIALLY DEFERRED"
        timingBuilder InitiallyImmediate = emit "INITIALLY IMMEDIATE"
        deferrableBuilder False = emit "NOT DEFERRABLE"
        deferrableBuilder True = emit "DEFERRABLE"

instance IsSql92ColumnConstraintDefinitionSyntax PgColumnConstraintDefinitionSyntax where
  type Sql92ColumnConstraintDefinitionConstraintSyntax PgColumnConstraintDefinitionSyntax = PgColumnConstraintSyntax
  type Sql92ColumnConstraintDefinitionAttributesSyntax PgColumnConstraintDefinitionSyntax = SqlConstraintAttributesBuilder

  constraintDefinitionSyntax nm constraint attrs =
    PgColumnConstraintDefinitionSyntax $
    maybe mempty (\nm -> emit "CONSTRAINT " <> pgQuotedIdentifier nm <> emit " " ) nm <>
    fromPgColumnConstraint constraint <>
    maybe mempty (\a -> emit " " <> fromSqlConstraintAttributes a) attrs

instance IsSql92ColumnConstraintSyntax PgColumnConstraintSyntax where
  type Sql92ColumnConstraintMatchTypeSyntax PgColumnConstraintSyntax = PgMatchTypeSyntax
  type Sql92ColumnConstraintReferentialActionSyntax PgColumnConstraintSyntax = PgReferentialActionSyntax
  type Sql92ColumnConstraintExpressionSyntax PgColumnConstraintSyntax = PgExpressionSyntax

  notNullConstraintSyntax = PgColumnConstraintSyntax (emit "NOT NULL")
  uniqueColumnConstraintSyntax = PgColumnConstraintSyntax (emit "UNIQUE")
  primaryKeyColumnConstraintSyntax = PgColumnConstraintSyntax (emit "PRIMARY KEY")
  checkColumnConstraintSyntax expr = PgColumnConstraintSyntax (emit "CHECK(" <> fromPgExpression expr <> emit ")")
  referencesConstraintSyntax tbl fields matchType onUpdate onDelete =
    PgColumnConstraintSyntax $
    emit "REFERENCES " <> pgQuotedIdentifier tbl <> emit "("
    <> pgSepBy (emit ", ") (map pgQuotedIdentifier fields) <> emit ")" <>
    maybe mempty (\m -> emit " " <> fromPgMatchType m) matchType <>
    maybe mempty (\a -> emit " ON UPDATE " <> fromPgReferentialAction a) onUpdate <>
    maybe mempty (\a -> emit " ON DELETE " <> fromPgReferentialAction a) onDelete

class IsPgInsertOnConflictSyntax insertOnConflict where
  type PgInsertOnConflictInsertOnConflictTargetSyntax insertOnConflict :: *
  type PgInsertOnConflictInsertOnConflictUpdateSyntax insertOnConflict :: *

  onConflictDefaultSyntax :: insertOnConflict
  onConflictUpdateSyntax :: PgInsertOnConflictInsertOnConflictTargetSyntax insertOnConflict
                         -> PgInsertOnConflictInsertOnConflictUpdateSyntax insertOnConflict
                         -> insertOnConflict
  onConflictDoNothingSyntax :: PgInsertOnConflictInsertOnConflictTargetSyntax insertOnConflict
                            -> insertOnConflict

instance IsPgInsertOnConflictSyntax PgInsertOnConflictSyntax where
    type PgInsertOnConflictInsertOnConflictTargetSyntax PgInsertOnConflictSyntax = PgInsertOnConflictTargetSyntax
    type PgInsertOnConflictInsertOnConflictUpdateSyntax PgInsertOnConflictSyntax = PgInsertOnConflictUpdateSyntax

    onConflictDefaultSyntax = PgInsertOnConflictSyntax mempty

    onConflictUpdateSyntax target update =
        PgInsertOnConflictSyntax $
        emit "ON CONFLICT " <> fromPgInsertOnConflictTarget target <> emit " DO UPDATE " <>
        fromPgInsertOnConflictUpdate update

    onConflictDoNothingSyntax target =
        PgInsertOnConflictSyntax $
        emit "ON CONFLICT " <> fromPgInsertOnConflictTarget target <> emit " DO NOTHING"

newtype PgInsertOnConflict insertOnConflict (tbl :: (* -> *) -> *) =
    PgInsertOnConflict insertOnConflict

onConflictDefault :: IsPgInsertOnConflictSyntax insertOnConflict =>
                     PgInsertOnConflict insertOnConflict tbl
onConflictDefault = PgInsertOnConflict onConflictDefaultSyntax

onConflictUpdate :: IsPgInsertOnConflictSyntax insertOnConflict =>
                    PgInsertOnConflictInsertOnConflictTargetSyntax insertOnConflict
                 -> PgInsertOnConflictInsertOnConflictUpdateSyntax insertOnConflict
                 -> PgInsertOnConflict insertOnConflict tbl
onConflictUpdate tgt update = PgInsertOnConflict $ onConflictUpdateSyntax tgt update

onConflictDoNothing :: IsPgInsertOnConflictSyntax insertOnConflict =>
                       PgInsertOnConflictInsertOnConflictTargetSyntax insertOnConflict
                    -> PgInsertOnConflict insertOnConflict tbl
onConflictDoNothing tgt = PgInsertOnConflict $ onConflictDoNothingSyntax tgt

-- instance Sql92Syntax PgSyntax where
--   type Sql92SelectSyntax PgSyntax
--       = PgSyntax
--   type Sql92UpdateSyntax PgSyntax = PgSyntax
--   type Sql92InsertSyntax PgSyntax = PgSyntax
--   type Sql92DeleteSyntax PgSyntax = PgSyntax

--   type Sql92ExpressionSyntax PgSyntax = PgSyntax
--   type Sql92ExpressionSyntax PgSyntax = PgSyntax
--   type Sql92ValueSyntax PgSyntax = PgSyntax

--   type Sql92FieldNameSyntax PgSyntax = PgSyntax

--   type Sql92ProjectionSyntax PgSyntax = PgSyntax
--   type Sql92FromSyntax PgSyntax = PgSyntax
--   type Sql92GroupingSyntax PgSyntax = PgSyntax
--   type Sql92OrderingSyntax PgSyntax = PgSyntax

--   type Sql92TableSourceSyntax PgSyntax = PgSyntax

--   type Sql92InsertValuesSyntax PgSyntax = PgSyntax

--   type Sql92AliasingSyntax PgSyntax = PgSyntax1

--   selectCmd = id

--   selectStmt _ proj from where_ grouping ordering limit offset =
--     emit "SELECT " <> proj <>
--     (maybe mempty (emit " FROM " <> ) from) <>
--     emit " WHERE " <> where_ <>
--     (maybe mempty (emit " GROUP BY" <>) grouping) <>
--     (case ordering of
--        [] -> mempty
--        ordering -> emit " ORDER BY " <> pgSepBy (emit ", ") ordering) <>
--     (maybe mempty (emit . fromString . (" LIMIT " <>) . show) limit) <>
--     (maybe mempty (emit . fromString . (" OFFSET " <>) . show) offset)

--   qualifiedFieldE _ a b =
--     pgQuotedIdentifier a <> emit "." <> pgQuotedIdentifier b
--   unqualifiedFieldE _ = pgQuotedIdentifier

--   addE _ = pgBinOp "+"
--   subE _ = pgBinOp "-"
--   mulE _ = pgBinOp "*"
--   divE _ = pgBinOp "/"
--   modE _ = pgBinOp "%"
--   orE _ = pgBinOp "OR"
--   andE _ = pgBinOp "AND"
--   eqE _ = pgBinOp "="
--   neqE _ = pgBinOp "<>"
--   ltE _ = pgBinOp "<"
--   gtE _ = pgBinOp ">"
--   leE _ = pgBinOp "<="
--   geE _ = pgBinOp ">="
--   negateE _ = pgUnOp "-"
--   notE _ = pgUnOp "NOT"
--   existsE _ = pgUnOp "EXISTS"
--   isJustE _ a =
--     emit "(" <> a <> emit ") IS NOT NULL"
--   isNothingE _ a =
--     emit "(" <> a <> emit ") IS NULL"
--   valueE _ = id
--   valuesE _ vs = emit "VALUES(" <> pgSepBy (emit ", ") vs <> emit ")"

--   trueV _ = emit "TRUE"
--   falseV _ = emit "FALSE"
--   stringV _ = pgBuildAction . pure . Pg.toField
--   numericV _ = pgBuildAction . pure . Pg.toField
--   rationalV p x = divE p (valueE p (numericV p (  numerator x)))
--                          (valueE p (numericV p (denominator x)))
--   nullV _ = emit "NULL"

--   fromTable _ tableSrc Nothing = tableSrc
--   fromTable _ tableSrc (Just nm) =
--     tableSrc <> emit " AS " <> pgQuotedIdentifier nm

--   innerJoin _ = pgJoin "INNER JOIN"
--   leftJoin _ = pgJoin "LEFT JOIN"
--   rightJoin _ = pgJoin "RIGHT JOIN"

--   tableNamed _ nm = pgQuotedIdentifier nm

--   projExprs _ exprs =
--     pgSepBy (emit ", ") (map buildPgSyntax1 exprs)

--   aliasExpr _ expr Nothing = PgSyntax1 expr
--   aliasExpr _ expr (Just lbl) = PgSyntax1 (expr <> emit " AS " <> pgQuotedIdentifier lbl)

--   insertSqlExpressions _ es =
--       emit "VALUES " <>
--       pgSepBy (emit ", ")
--               (map (\es -> emit "(" <> pgSepBy (emit ", ") es <> emit ")") es)
--   insertFrom _ = id

instance Pg.ToField a => HasSqlValueSyntax PgValueSyntax a where
  sqlValueSyntax =
    PgValueSyntax . pgBuildAction . pure . Pg.toField

pgQuotedIdentifier :: T.Text -> PgSyntax
pgQuotedIdentifier t =
  emit "\"" <>
  (emit . TE.encodeUtf8 $
   T.concatMap quoteIdentifierChar t) <>
  emit "\""
  where
    quoteIdentifierChar '"' = "\"\""
    quoteIdentifierChar c = T.singleton c

pgParens :: PgSyntax -> PgSyntax
pgParens a = emit "(" <> a <> emit ")"

pgTableOp :: ByteString -> PgSelectTableSyntax -> PgSelectTableSyntax
          -> PgSelectTableSyntax
pgTableOp op tbl1 tbl2 =
    PgSelectTableSyntax $
    emit "(" <> fromPgSelectTable tbl1 <> emit ") " <> emit op <>
    emit " (" <> fromPgSelectTable tbl2 <> emit ")"

pgCompOp :: ByteString -> Maybe PgComparisonQuantifierSyntax
         -> PgExpressionSyntax -> PgExpressionSyntax -> PgExpressionSyntax
pgCompOp op quantifier a b =
  PgExpressionSyntax $
  emit "(" <> fromPgExpression a <>
  emit (") " <> op <> " (") <>
  maybe mempty (\q -> emit " " <> fromPgComparisonQuantifier q <> emit " ") quantifier <>
  fromPgExpression b <> emit ")"

pgBinOp :: ByteString -> PgExpressionSyntax -> PgExpressionSyntax -> PgExpressionSyntax
pgBinOp op a b =
  PgExpressionSyntax $
  emit "(" <> fromPgExpression a <> emit (") " <> op <> " (") <> fromPgExpression b <> emit ")"

pgPostFix, pgUnOp :: ByteString -> PgExpressionSyntax -> PgExpressionSyntax
pgPostFix op a =
  PgExpressionSyntax $
  emit "(" <> fromPgExpression a <> emit ") " <> emit op
pgUnOp op a =
  PgExpressionSyntax $
  emit (op <> "(") <> fromPgExpression a <> emit ")"

pgJoin :: ByteString -> PgFromSyntax -> PgFromSyntax -> Maybe PgExpressionSyntax -> PgFromSyntax
pgJoin joinType a b Nothing =
  PgFromSyntax $
  fromPgFrom a <> emit (" " <> joinType <> " ") <> fromPgFrom b
pgJoin joinType a b (Just on) =
  PgFromSyntax $
  fromPgFrom (pgJoin joinType a b Nothing) <> emit " ON " <> fromPgExpression on

pgSepBy :: PgSyntax -> [PgSyntax] -> PgSyntax
pgSepBy _ [] = mempty
pgSepBy _ [x] = x
pgSepBy sep (x:xs) = x <> sep <> pgSepBy sep xs

pgDebugRenderSyntax :: PgSyntax -> IO ()
pgDebugRenderSyntax (PgSyntax p) = go p Nothing
  where go :: PgSyntaxM () -> Maybe (PgSyntaxF ()) -> IO ()
        go p = runF p finish step
        step x lastBs =
          case (x, lastBs) of
            (EmitBuilder s next, lastBs) ->
              step (EmitByteString (toStrict (toLazyByteString s)) next) lastBs
            (x, Nothing) ->
              nextSyntaxStep x (Just (fmap (const ()) x))
            (EmitByteString x next, Just (EmitByteString before _)) ->
              next (Just (EmitByteString (before <> x) ()))
            (EscapeString x next, Just (EscapeString before _)) ->
              next (Just (EscapeString (before <> x) ()))
            (EscapeBytea x next, Just (EscapeBytea before _)) ->
              next (Just (EscapeBytea (before <> x) ()))
            (EscapeIdentifier x next, Just (EscapeIdentifier before _)) ->
              next (Just (EscapeIdentifier (before <> x) ()))
            (s, Just e) ->
              renderStep e >>
              nextSyntaxStep s (Just (fmap (const ()) s))

        renderStep (EmitByteString x _) = putStrLn ("EmitByteString " <> show x)
        renderStep (EscapeString x _) = putStrLn ("EscapeString " <> show x)
        renderStep (EscapeBytea x _) = putStrLn ("EscapeBytea " <> show x)
        renderStep (EscapeIdentifier x _) = putStrLn ("EscapeIdentifier " <> show x)

        finish x Nothing = pure x
        finish x (Just s) = renderStep s >> pure x

pgBuildAction :: [ Pg.Action ] -> PgSyntax
pgBuildAction =
  foldMap $ \action ->
  case action of
    Pg.Plain x -> emitBuilder x
    Pg.Escape str -> emit "'" <> escapeString str <> emit "'"
    Pg.EscapeByteA bin -> emit "'" <> escapeBytea bin <> emit "'"
    Pg.EscapeIdentifier id -> escapeIdentifier id
    Pg.Many as -> pgBuildAction as

-- * Postrges-specific extensions

-- class Sql92Syntax syntax => PgExtensionsSyntax syntax where
--   tableSampleSyntax ::
--        Proxy syntax -> T.Text -> Maybe T.Text -> PgTableSamplingMethod
--     -> [Sql92ExpressionSyntax syntax] {-^ Arguments to sampling method -}
--     -> Maybe (Sql92ExpressionSyntax syntax) {-^ Seed -}
--     -> Sql92FromSyntax syntax

-- instance PgExtensionsSyntax PgSyntax where
--   tableSampleSyntax _ tblName tblAlias (PgTableSamplingMethod sampleMethod) sampleMethodParams seedExpr =
--     pgQuotedIdentifier tblName <>
--     maybe mempty (\x -> emit " AS " <> pgQuotedIdentifier x) tblAlias <>
--     emit " TABLESAMPLE " <> emit (TE.encodeUtf8 sampleMethod) <> emit "(" <> pgSepBy (emit ", ") sampleMethodParams <> emit ")" <>
--     maybe mempty (\x -> emit " REPEATABLE (" <> x <> emit ")") seedExpr

-- * Postgres specific commands

insert :: DatabaseEntity Postgres be (TableEntity table)
       -> SqlInsertValues PgInsertValuesSyntax table
       -> PgInsertOnConflict PgInsertOnConflictSyntax table
       -> SqlInsert PgInsertSyntax
insert tbl values onConflict =
    let PgInsertReturning a = insertReturning tbl values onConflict (Nothing :: Maybe (table (QExpr PgExpressionSyntax PostgresInaccessible) -> QExpr PgExpressionSyntax PostgresInaccessible Int))
    in SqlInsert (PgInsertSyntax a)

newtype PgInsertReturning a = PgInsertReturning PgSyntax

insertReturning :: Projectible PgExpressionSyntax a
                => DatabaseEntity Postgres be (TableEntity table)
                -> SqlInsertValues PgInsertValuesSyntax table
                -> PgInsertOnConflict PgInsertOnConflictSyntax table
                -> Maybe (table (QExpr PgExpressionSyntax PostgresInaccessible) -> a)
                -> PgInsertReturning (QExprToIdentity a)

insertReturning (DatabaseEntity (DatabaseTable tblNm tblSettings))
                (SqlInsertValues (PgInsertValuesSyntax insertValues))
                (PgInsertOnConflict (PgInsertOnConflictSyntax onConflict))
                returning =
  PgInsertReturning $
  emit "INSERT INTO " <> pgQuotedIdentifier tblNm <>
  emit "(" <> pgSepBy (emit ", ") (allBeamValues (\(Columnar' f) -> pgQuotedIdentifier (_fieldName f)) tblSettings) <> emit ") " <>
  insertValues <> emit " " <> onConflict <>
  (case returning of
     Nothing -> mempty
     Just mkProjection ->
         let tblQ = changeBeamRep (\(Columnar' f) -> Columnar' (QExpr (fieldE (unqualifiedField (_fieldName f))))) tblSettings
         in emit " RETURNING "<>
            pgSepBy (emit ", ") (map fromPgExpression (project (mkProjection tblQ))))

-- -- * Pg-specific Q monad

-- bernoulliSample ::
--   forall syntax db table s.
--   ( Database db, SupportedSyntax Postgres syntax, PgExtensionsSyntax syntax) =>
--   DatabaseTable Postgres db table -> QExpr syntax s Double ->
--   Q syntax db s (table (QExpr syntax s))
-- bernoulliSample
--   tbl@(DatabaseTable table name tableSettings :: DatabaseTable Postgres db table)
--   (QExpr prob) =

--   do curTbl <- gets qbNextTblRef
--      let newSource = tableSampleSyntax (Proxy @syntax) name
--                                        (Just (fromString ("t" <> show curTbl)))
--                                        pgBernoulliSamplingMethod
--                                        [ prob ]
--                                        Nothing
--      buildJoinFrom tbl newSource Nothing

now_ :: QExpr PgExpressionSyntax s LocalTime
now_ = QExpr (PgExpressionSyntax (emit "NOW()"))
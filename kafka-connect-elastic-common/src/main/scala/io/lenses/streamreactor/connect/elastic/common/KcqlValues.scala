/*
 * Copyright 2017-2026 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.elastic.common

import io.lenses.kcql.targettype.TargetType
import io.lenses.sql.Field
import io.lenses.streamreactor.connect.elastic.common.NullValueBehavior.NullValueBehavior
import io.lenses.streamreactor.connect.elastic.common.indexname.CustomIndexName

/**
 * `targetType` and `indexSuffix` are resolved once at [[io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter]]
 * construction, from KCQL-only inputs (`Kcql.getTargetType()`, `Kcql.getIndexSuffix()`) that
 * never vary per record. `targetType` has no default: `CreateIndex.extractTargetFromMessage`
 * pattern-matches on it with no default case, so a `null` would produce a `MatchError` — which is
 * `NonFatal`, and therefore silently DIVERTIBLE under `bulk.item.errors=drop`/`dlq` rather than
 * failing loudly, exactly the kind of KCQL-level fault this file's construction-time validation
 * exists to prevent. `JsonBulkWriter` always supplies a real one; tests exercising
 * `Transform`/`TransformAndExtractPK` directly must supply a placeholder explicitly.
 */
case class KcqlValues(
  fields:               Seq[Field],
  ignoredFields:        Seq[Field],
  primaryKeysPath:      Seq[Vector[String]],
  behaviorOnNullValues: NullValueBehavior,
  targetType:           TargetType,
  indexSuffix:          Option[CustomIndexName] = None,
)

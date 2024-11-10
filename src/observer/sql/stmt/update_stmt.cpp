/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/update_stmt.h"
#include "common/log/log.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/parser/expression_binder.h"

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{
  BinderContext binder_context;

  const char *table_name = update.relation_name.c_str();
  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("invalid argument. db=%p, table_name=%p", db, table_name);
    return RC::INVALID_ARGUMENT;
  }

// check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  
// check whether the field exists
  vector<const FieldMeta*> fields;
  vector<Value> values;
  for (auto &attr : update.attribute_names) {
    const FieldMeta *field_meta = table->table_meta().field(attr.first.c_str());
    if (nullptr == field_meta) {
      LOG_WARN("no such field in table. db=%s, table=%s, field name=%s", db->name(), table_name, attr.first.c_str());
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }
    fields.push_back(field_meta);
    values.push_back(attr.second);
  }
  
// copy from delete
  binder_context.add_table(table);
  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(std::string(table_name), table));

  ExpressionBinder expression_binder(binder_context);
  vector<unique_ptr<Expression>> bound_condition;
  std::unique_ptr<Expression> smd(update.conditions);
  if(nullptr == update.conditions){
    bound_condition.push_back(nullptr);
    LOG_INFO("sql_condition is null");
  }
  else{
    RC rrc = expression_binder.bind_expression(smd, bound_condition);
    if (OB_FAIL(rrc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rrc));
      return rrc;
    }
    ASSERT(bound_condition.size() == 1, "invalid condition size");
  }

  FilterStmt *filter_stmt = nullptr;
  RC          rc          = FilterStmt::create(
      db, table, &table_map, std::move(bound_condition[0]), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  stmt = new UpdateStmt(table, fields, values, filter_stmt);
  return rc;
}

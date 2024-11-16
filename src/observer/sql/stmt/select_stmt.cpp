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
// Created by Wangyunlai on 2022/6/6.
//

#include "sql/stmt/select_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/parser/expression_binder.h"

using namespace std;
using namespace common;


RC find_table_by_name(Db *db,const char* table_name,Table*& table){
  if (nullptr == table_name) {
    LOG_WARN("invalid argument. relation name is null.");
    return RC::INVALID_ARGUMENT;
  }
  table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  return RC::SUCCESS;
}

RC bound_conditions(ExpressionBinder &expression_binder,Expression* condition,std::vector<std::unique_ptr<Expression>> &bound_condition)
{
  std::unique_ptr<Expression> smd(condition);
  if(nullptr == condition){
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
  return RC::SUCCESS;
}

RC my_get_relation(Db *db,vector<Table *> &tables,BinderContext& binder_context,
unordered_map<string, Table *> &table_map,std::string relation, InnerJoinChain& join_chain, Expression* condition){
  Table* table = nullptr;
  RC rc = find_table_by_name(db,relation.c_str(),table);
  if (OB_FAIL(rc)) {
    LOG_WARN("find table failed. rc=%s", strrc(rc));
    return rc;
  }
  tables.push_back(table);
  binder_context.add_table(table);
  table_map.insert({relation, table});
  //bound the condition
  std::vector<std::unique_ptr<Expression>> bound_condition;
  ExpressionBinder expression_binder(binder_context);
  bound_conditions(expression_binder,condition,bound_condition);

  if(bound_condition.empty()){
    return RC::INVALID_ARGUMENT;
  }

  FilterStmt* filter_stmt = nullptr;
  if(condition!=nullptr){
    rc = FilterStmt::create(db, table, &table_map, std::move(bound_condition[0]), filter_stmt);
    if (rc != RC::SUCCESS) {
      LOG_WARN("cannot construct filter stmt");
      return rc;
    }
  }
  join_chain.add_table(table,filter_stmt);
  return RC::SUCCESS;
}

RC shamama(Db *db,vector<InnerJoinSqlNode> &relations,BinderContext& binder_context,vector<Table *> &tables,
  unordered_map<string, Table *> &table_map,std::vector<InnerJoinChain> &join_tables)
{
  for (size_t i = 0; i < relations.size(); i++) {
    const InnerJoinSqlNode& relation = relations[i];
    InnerJoinChain join_chain;

    for (size_t j = 0; j < relation.relations.size(); ++j) {
      RC rc = my_get_relation(db,tables,binder_context,table_map,relation.relations[j], join_chain, j?relation.conditions[j-1]:nullptr);
      if (OB_FAIL(rc)) {
        LOG_WARN("Process Base Relation %s Failed!", relation.relations[i].c_str());
        return rc;
      }
    }

    join_tables.emplace_back(join_chain);
  }
  return RC::SUCCESS;
  
}

RC SelectStmt::create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  BinderContext binder_context;

  // collect tables in `from` statement
  vector<Table *>                tables;
  unordered_map<string, Table *> table_map;
  std::vector<InnerJoinChain> join_tables;

  RC rc = shamama(db,select_sql.relations,binder_context,tables,table_map,join_tables);
  if (OB_FAIL(rc)) {
    LOG_INFO("shamama failed. rc=%s", strrc(rc));
    return rc;
  }

  // for (size_t i = 0; i < select_sql.relations.size(); i++) {
  //   const InnerJoinSqlNode& relations = select_sql.relations[i];

  //   const char *table_name = relations.relations[0].c_str();

  //   if (nullptr == table_name) {
  //     LOG_WARN("invalid argument. relation name is null. index=%d", i);
  //     return RC::INVALID_ARGUMENT;
  //   }

  //   Table *table = db->find_table(table_name);
  //   if (nullptr == table) {
  //     LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
  //     return RC::SCHEMA_TABLE_NOT_EXIST;
  //   }

  //   //TODO: implement join

  //   binder_context.add_table(table);
  //   tables.push_back(table);
  //   table_map.insert({table_name, table});
  // }

  // collect query fields in `select` statement
  vector<unique_ptr<Expression>> bound_expressions;
  ExpressionBinder expression_binder(binder_context);

  for (std::unique_ptr<Expression> &expression : select_sql.expressions) {
    //output the type of expression
    RC rc = expression_binder.bind_expression(expression, bound_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }

  std::vector<std::unique_ptr<Expression>> bound_condition;
  bound_conditions(expression_binder,select_sql.conditions,bound_condition);

  vector<unique_ptr<Expression>> group_by_expressions;
  for (std::unique_ptr<Expression> &expression : select_sql.group_by) {
    RC rc = expression_binder.bind_expression(expression, group_by_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }
  std::vector<std::pair<bool,std::unique_ptr<Expression>>> order_by_expressions;
  for (auto &expression : select_sql.order_by) {
    std::unique_ptr<Expression> expr_ptr(expression.second);
    vector<unique_ptr<Expression>> one_expr;
    RC rc = expression_binder.bind_expression(expr_ptr, one_expr);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
    order_by_expressions.push_back({expression.first, std::move(one_expr[0])});
  }

  vector<unique_ptr<Expression>> having_condition;
  std::unique_ptr<Expression> having_expr(select_sql.having);
  if(nullptr == select_sql.having){
    having_condition.push_back(nullptr);
    LOG_INFO("sql_having is null");
  }
  else{
    RC rrc = expression_binder.bind_expression(having_expr, having_condition);
    if (OB_FAIL(rrc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rrc));
      return rrc;
    }
    ASSERT(having_condition.size() == 1, "invalid having size");
  }

  Table *default_table = nullptr;
  if (tables.size() == 1) {
    default_table = tables[0];
  }

  if(bound_condition.empty()){
    return RC::INVALID_ARGUMENT;
  }

  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  RC          rrc          = FilterStmt::create(db,
      default_table,
      &table_map,
      std::move(bound_condition[0]),
      filter_stmt);
  if (rrc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  // everything alright
  SelectStmt *select_stmt = new SelectStmt();

  select_stmt->tables_j.swap(join_tables);
  select_stmt->query_expressions_.swap(bound_expressions);
  select_stmt->filter_stmt_ = filter_stmt;
  select_stmt->group_by_.swap(group_by_expressions);
  select_stmt->order_by_.swap(order_by_expressions);
//  select_stmt->having_ = having_condition[0].release();
  stmt                      = select_stmt;
  return RC::SUCCESS;
}

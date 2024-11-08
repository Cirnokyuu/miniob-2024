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
// Created by WangYunlai on 2024/05/29.
//

#pragma once

#include "sql/operator/logical_operator.h"

class OrderByLogicalOperator : public LogicalOperator
{
public:
  OrderByLogicalOperator(
      std::vector<pair<bool,std::unique_ptr<Expression>>> &&order_by_exprs,
      std::vector<Expression*>& query_exprressions
      );

  virtual ~OrderByLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::ORDER_BY; }

	std::vector<pair<bool,std::unique_ptr<Expression>>>& order_by_expressions(){
			return order_by_expressions_;
	}
  std::vector<Expression*>& query_exprressions(){
    return query_exprressions_;
  }
	
private:
  std::vector<pair<bool,std::unique_ptr<Expression>>> order_by_expressions_;
  std::vector<Expression*> query_exprressions_;
  
};
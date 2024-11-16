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

#include "sql/operator/physical_operator.h"
#include "sql/expr/composite_tuple.h"

/**
 * @brief Order By 物理算子基类
 * @ingroup PhysicalOperator
 */
class OrderByPhysicalOperator : public PhysicalOperator
{
public:
  OrderByPhysicalOperator(
    std::vector<pair<bool,std::unique_ptr<Expression>>> &&order_by_exprs,
    std::vector<Expression*>& query_exprressions);
  virtual ~OrderByPhysicalOperator() = default;
	
	std::vector<pair<bool,std::unique_ptr<Expression>>>& order_by_expressions(){
		return order_by_expressions_;
	}

  PhysicalOperatorType type() const override{
    return PhysicalOperatorType::ORDER_BY;
  }
  void set_names(std::vector<TupleCellSpec>& names, Expression* expr);
  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;
  Tuple* current_tuple() override;
  RC work();

protected:
  std::vector<pair<bool,std::unique_ptr<Expression>>> order_by_expressions_;
  std::vector<Expression*> query_exprressions_;
  std::vector<Expression*> name_exprs;
  std::vector<int> answer;
  std::vector<std::vector<Value>> ans_values;
  std::vector<std::vector<Value>> key_values;
  int now_index = 0;
  ValueListTuple cur_tuple;

  RC get_key_values();
};
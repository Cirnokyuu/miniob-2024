#include "sql/operator/physical_operator.h"
#include "sql/expr/expression.h"
#include "sql/operator/order_by_physical_operator.h"
#include "sql/expr/tuple.h"
#include <algorithm>


void OrderByPhysicalOperator::set_names(std::vector<TupleCellSpec>& names, Expression* expr){
  if(expr == nullptr){
    return;
  }
	if(expr->type()==ExprType::FIELD){
	  FieldExpr* field_expr = static_cast<FieldExpr*>(expr);
	  TupleCellSpec spec = TupleCellSpec(field_expr->table_name(),field_expr->field_name());
	  names.push_back(spec);
    name_exprs.push_back(field_expr);
	}
	else if(expr->type()==ExprType::AGGREGATION){
    AggregateExpr* agg_expr = static_cast<AggregateExpr*>(expr);
	  TupleCellSpec spec = TupleCellSpec(expr->name());
	  names.push_back(spec);
    name_exprs.push_back(agg_expr);
	}
	else if(expr->type() == ExprType::VALUE){
	}
	else if(expr->type() == ExprType::CAST){
		CastExpr* cast_expr = static_cast<CastExpr*>(expr);
		set_names(names, cast_expr->child().get());
	}
	else if(expr->type() == ExprType::COMPARISON){
    ComparisonExpr* comparison_expr = static_cast<ComparisonExpr*>(expr);
    set_names(names, comparison_expr->left().get());
    set_names(names, comparison_expr->right().get());
	}
	else if(expr->type() == ExprType::CONJUNCTION){
    ConjunctionExpr* conjunction_expr = static_cast<ConjunctionExpr*>(expr);
    for(auto &child: conjunction_expr->children()){
      set_names(names, child.get());
    }
  }
  else if(expr->type() == ExprType::ARITHMETIC){
    ArithmeticExpr* arithmetic_expr = static_cast<ArithmeticExpr*>(expr);
    set_names(names, arithmetic_expr->left().get());
    set_names(names, arithmetic_expr->right().get());
  }
}

OrderByPhysicalOperator::OrderByPhysicalOperator(
  std::vector<pair<bool,std::unique_ptr<Expression>>> &&order_by_exprs,
  std::vector<Expression*>& query_exprressions)
{
  order_by_expressions_ = std::move(order_by_exprs);
  query_exprressions_ = query_exprressions;
  std::vector<TupleCellSpec> names;
  for(auto &expr: query_exprressions_){
	  set_names(names,expr);
  }
  cur_tuple.set_names(names);
}

RC OrderByPhysicalOperator::open(Trx *trx){
  if(children_.size()==0){
    return RC::SUCCESS;
  }
  RC rc = children_[0]->open(trx);
  if(OB_FAIL(rc)){
    return RC::INTERNAL;
  }
  rc=work();
  return rc;
}

RC OrderByPhysicalOperator::get_key_values(){
	while(children_[0]->next() == RC::SUCCESS){
		std::vector<Value> k_values,all_values;
		for(auto &expr: order_by_expressions_){
			Value value;
			RC rc = expr.second->get_value(*children_[0]->current_tuple(), value);
			//output the type of value
			ASSERT(value.attr_type() != AttrType::UNDEFINED, "value type is undefined");
			LOG_DEBUG("value %d", value.attr_type());
			if(OB_FAIL(rc)){
				return rc;
			}
			k_values.push_back(value);
		}
		key_values.push_back(k_values);
		for(auto &expr: name_exprs){
			Value value;
			RC rc = expr->get_value(*children_[0]->current_tuple(), value);
			ASSERT(value.attr_type() != AttrType::UNDEFINED, "value type is undefined");
			if(OB_FAIL(rc)){
				return rc;
			}
			all_values.push_back(value);
		}
		ans_values.push_back(all_values);
		answer.push_back(ans_values.size()-1);
	}
	return RC::SUCCESS;
}
RC OrderByPhysicalOperator::work(){
  now_index = 0;
	answer.clear();
	key_values.clear();
	RC rc=get_key_values();
	if(OB_FAIL(rc)){
		return rc;
	}
	std::sort(answer.begin(),answer.end(),
		[&](int a,int b){
			auto &a_vec=key_values[a];
			auto &b_vec=key_values[b];
			for(int i=0;i<order_by_expressions_.size();i++){
				bool null_a=a_vec[i].is_null();
				bool null_b=b_vec[i].is_null();
				if(null_a){
					if(null_b)continue;
					return order_by_expressions_[i].first;
				}
				if(null_b)return !order_by_expressions_[i].first;
				int result=a_vec[i].compare(b_vec[i]);
				if(result!=0){
					if(order_by_expressions_[i].first){
						return result<0;
					}else{
						return result>0;
					}
				}
			}
			return a<b;
		}
	);
	return RC::SUCCESS;
}

RC OrderByPhysicalOperator::next(){
  if(now_index<answer.size()){
    cur_tuple.set_cells(ans_values[answer[now_index]]);
  	LOG_DEBUG("cur_tuple %s", cur_tuple.to_string().c_str());
    now_index++;
    return RC::SUCCESS;
  }
  return RC::RECORD_EOF;
}

RC OrderByPhysicalOperator::close(){
  return children_[0]->close();
}

Tuple* OrderByPhysicalOperator::current_tuple(){
  //output the values in cur_tuple
  LOG_DEBUG("cur_tuple %s", cur_tuple.to_string().c_str());
	return &cur_tuple;
}
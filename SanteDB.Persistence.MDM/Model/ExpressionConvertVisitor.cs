using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace SanteDB.Persistence.MDM.Model
{
    /// <summary>
    /// Converts <typeparamref name="TFrom"/> to <typeparamref name="TTo"/>
    /// </summary>
    /// <remarks>
    /// Quick and dirty solution from https://stackoverflow.com/questions/14437239/change-a-linq-expression-predicate-from-one-type-to-another
    /// </remarks>
    public class ExpressionConvertVisitor<TFrom, TTo, TReturn> : ExpressionVisitor
    {

        private Dictionary<string, ParameterExpression> convertedParameters;
        private Expression<Func<TFrom, TReturn>> expression;

        public ExpressionConvertVisitor(Expression<Func<TFrom, TReturn>> expresionToConvert)
        {
            //for each parameter in the original expression creates a new parameter with the same name but with changed type 
            convertedParameters = expresionToConvert.Parameters
                .ToDictionary(
                    x => x.Name,
                    x => Expression.Parameter(typeof(TTo), x.Name)
                );

            expression = expresionToConvert;
        }

        public Expression<Func<TTo, TReturn>> Convert()
        {
            return (Expression<Func<TTo, TReturn>>)Visit(expression);
        }

        //handles Properties and Fields accessors 
        protected override Expression VisitMember(MemberExpression node)
        {
                    
            if (node.Member.DeclaringType == typeof(TFrom))
            {
                var memeberInfo = typeof(TTo).GetMember(node.Member.Name).First();
                var newExp = Visit(node.Expression);
                return Expression.MakeMemberAccess(newExp, memeberInfo);
            }
            else
            {
                return base.VisitMember(node);
            }
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            var newParameter = convertedParameters[node.Name];
            return newParameter;
        }

        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            var newExp = Visit(node.Body);
            return Expression.Lambda(newExp, convertedParameters.Select(x => x.Value));
        }
    }
}

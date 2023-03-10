/*
 * Copyright (C) 2021 - 2023, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
 * Copyright (C) 2019 - 2021, Fyfe Software Inc. and the SanteSuite Contributors
 * Portions Copyright (C) 2015-2018 Mohawk College of Applied Arts and Technology
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. You may 
 * obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the 
 * License for the specific language governing permissions and limitations under 
 * the License.
 * 
 * User: fyfej
 * Date: 2023-3-10
 */
using SanteDB.Core.i18n;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Model;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Security.Principal;

namespace SanteDB.Persistence.MDM.Services.Resources
{
    /// <summary>
    /// A delay load query wrapper that synthesizes a series of mdm masters
    /// </summary>
    public class MdmEntityResultSet<TModel> : IQueryResultSet<TModel>, IOrderableQueryResultSet<TModel>
        where TModel : Entity, new()
    {
        // Wrapped result set
        private readonly IQueryResultSet<Entity> m_wrappedResultSet;

        // Principal consuming this result set
        private readonly IPrincipal m_principal;

        /// <inheritdoc/>
        public Type ElementType => typeof(TModel);

        /// <summary>
        /// Creates a new mdm query result set
        /// </summary>
        /// <param name="wrappedResultSet">The result set as filtered ready to be converted to <see cref="EntityMaster{T}"/></param>
        internal MdmEntityResultSet(IQueryResultSet<Entity> wrappedResultSet, IPrincipal asPrincipal)
        {
            this.m_wrappedResultSet = wrappedResultSet;
            this.m_principal = asPrincipal;
        }

        /// <summary>
        /// True if this set has any results
        /// </summary>
        public bool Any() => this.m_wrappedResultSet.Any();

        /// <inheritdoc/>
        public IQueryResultSet<TModel> Distinct() => new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.Distinct(), this.m_principal);

        /// <summary>
        /// Get this result set as a stateful query
        /// </summary>
        public IQueryResultSet<TModel> AsStateful(Guid stateId) => new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.AsStateful(stateId), this.m_principal);

        /// <summary>
        /// Gets the count of this query
        /// </summary>
        public int Count() => this.m_wrappedResultSet.Count();

        /// <summary>
        /// Get the first result set
        /// </summary>
        public TModel First()
        {
            using (DataPersistenceControlContext.Create(LoadMode.SyncLoad))
            {
                return new EntityMaster<TModel>(this.m_wrappedResultSet.First()).Synthesize(this.m_principal);
            }
        }

        /// <summary>
        /// Get the first or default
        /// </summary>
        public TModel FirstOrDefault()
        {
            using (DataPersistenceControlContext.Create(LoadMode.SyncLoad))
            {
                var fd = this.m_wrappedResultSet.FirstOrDefault();
                if (fd == null)
                {
                    return null;
                }
                else
                {
                    return new EntityMaster<TModel>(fd).Synthesize(this.m_principal);
                }
            }
        }

        /// <summary>
        /// Get the enumerator for this object
        /// </summary>
        public IEnumerator<TModel> GetEnumerator()
        {
            using (DataPersistenceControlContext.Create(LoadMode.SyncLoad))
            {
                foreach (var itm in this.m_wrappedResultSet)
                {
                    yield return new EntityMaster<TModel>(itm).Synthesize(this.m_principal);
                }
            }
        }

        /// <summary>
        /// Retrieve a single object
        /// </summary>
        public TModel Single()
        {
            using (DataPersistenceControlContext.Create(LoadMode.SyncLoad))
            {
                return new EntityMaster<TModel>(this.m_wrappedResultSet.Single()).Synthesize(this.m_principal);
            }
        }

        /// <summary>
        /// Get single or default
        /// </summary>
        public TModel SingleOrDefault()
        {
            using (DataPersistenceControlContext.Create(LoadMode.SyncLoad))
            {
                var sd = this.m_wrappedResultSet.SingleOrDefault();
                if (sd == null)
                {
                    return null;
                }
                else
                {
                    return new EntityMaster<TModel>(sd).Synthesize(this.m_principal);
                }
            }
        }

        /// <summary>
        /// Skip the number of results
        /// </summary>
        public IQueryResultSet<TModel> Skip(int count) => new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.Skip(count), this.m_principal);

        /// <summary>
        /// Take the number of results
        /// </summary>
        public IQueryResultSet<TModel> Take(int count) => new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.Take(count), this.m_principal);

        /// <summary>
        /// Filter according to a where clause
        /// </summary>
        public IQueryResultSet<TModel> Where(Expression<Func<TModel, bool>> query)
        {
            return new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.Where(new ExpressionParameterRewriter<TModel, Entity, bool>(query).Convert()), this.m_principal);
        }

        #region Non-Generic

        /// <summary>
        /// Get this as a stateful result set
        /// </summary>
        IQueryResultSet IQueryResultSet.AsStateful(Guid stateId) => this.AsStateful(stateId);

        /// <summary>
        /// Get the first object
        /// </summary>
        object IQueryResultSet.First() => this.First();

        /// <summary>
        /// Get the first or default
        /// </summary>
        object IQueryResultSet.FirstOrDefault() => this.FirstOrDefault();

        /// <summary>
        /// Get non-generic enumerator
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator() => this.GetEnumerator();

        /// <summary>
        /// Get the first and only result (or throw)
        /// </summary>
        object IQueryResultSet.Single() => this.Single();

        /// <summary>
        /// Get the first an only result (or throw) returning null if no results
        /// </summary>
        object IQueryResultSet.SingleOrDefault() => this.SingleOrDefault();

        /// <summary>
        /// Skip the <paramref name="count"/> results
        /// </summary>
        IQueryResultSet IQueryResultSet.Skip(int count) => this.Skip(count);

        /// <summary>
        /// Take the <paramref name="count"/> results
        /// </summary>
        IQueryResultSet IQueryResultSet.Take(int count) => this.Take(count);

        /// <summary>
        /// Non-generic select method
        /// </summary>
        public IEnumerable<TReturn> Select<TReturn>(Expression selector)
        {
            if (selector is Expression<Func<TModel, TReturn>> se)
            {
                return this.Select(se);
            }
            else if (selector is Expression<Func<TModel, dynamic>> de)
            {
                // Strip body convert
                return this.Select(Expression.Lambda<Func<TModel, TReturn>>(Expression.Convert(de.Body, typeof(TReturn)).Reduce(), de.Parameters));
            }
            else
            {
                throw new NotSupportedException(String.Format(ErrorMessages.ARGUMENT_INCOMPATIBLE_TYPE, typeof(Expression<Func<TModel, TReturn>>), selector.GetType()));
            }
        }

        /// <summary>
        /// Select specified data from entity master - note this will return only from the MASTER not a synthesized result
        /// </summary>
        public IEnumerable<TReturn> Select<TReturn>(Expression<Func<TModel, TReturn>> selector)
        {
            return this.m_wrappedResultSet.Select(new ExpressionParameterRewriter<TModel, Entity, TReturn>(selector).Convert());
        }

        /// <summary>
        /// Order by specified <paramref name="sortExpression"/>
        /// </summary>
        public IOrderableQueryResultSet<TModel> OrderBy<TKey>(Expression<Func<TModel, TKey>> sortExpression)
        {
            if (this.m_wrappedResultSet is IOrderableQueryResultSet<Entity> orderable)
            {
                return new MdmEntityResultSet<TModel>(orderable.OrderBy(new ExpressionParameterRewriter<TModel, Entity, TKey>(sortExpression).Convert()), this.m_principal);
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.NOT_SUPPORTED_IMPLEMENTATION, typeof(IOrderableQueryResultSet<TModel>)));
            }
        }

        /// <summary>
        /// Order result set by descending
        /// </summary>
        public IOrderableQueryResultSet<TModel> OrderByDescending<TKey>(Expression<Func<TModel, TKey>> sortExpression)
        {
            if (this.m_wrappedResultSet is IOrderableQueryResultSet<Entity> orderable)
            {
                return new MdmEntityResultSet<TModel>(orderable.OrderByDescending(new ExpressionParameterRewriter<TModel, Entity, TKey>(sortExpression).Convert()), this.m_principal);
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.NOT_SUPPORTED_IMPLEMENTATION, typeof(IOrderableQueryResultSet<TModel>)));
            }
        }

        /// <summary>
        /// Return as a stateful query
        /// </summary>
        IQueryResultSet<TModel> IQueryResultSet<TModel>.AsStateful(Guid stateId)
        {
            return new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.AsStateful(stateId), this.m_principal);
        }

        /// <summary>
        /// Select the specified object
        /// </summary>
        IEnumerable<TReturn> IQueryResultSet<TModel>.Select<TReturn>(Expression<Func<TModel, TReturn>> selector)
        {
            return this.m_wrappedResultSet.Select(new ExpressionParameterRewriter<TModel, Entity, TReturn>(selector).Convert());
        }

        /// <summary>
        /// Where condition
        /// </summary>
        public IQueryResultSet Where(Expression query)
        {
            if (query is Expression<Func<TModel, bool>> expr)
            {
                return this.Where(expr);
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.INVALID_EXPRESSION_TYPE, typeof(Expression<Func<TModel, bool>>), query.GetType()));
            }
        }

        /// <summary>
        /// Return true if the set contains any results
        /// </summary>
        /// <returns></returns>
        bool IQueryResultSet.Any() => this.Any();

        /// <summary>
        /// Return the count of results
        /// </summary>
        int IQueryResultSet.Count() => this.Count();

        /// <summary>
        /// Order by generic expression
        /// </summary>
        public IOrderableQueryResultSet OrderBy(Expression expression)
        {
            if (expression is Expression<Func<TModel, dynamic>> srt)
            {
                return this.OrderBy(srt);
            }
            else
            {
                throw new Exception(String.Format(ErrorMessages.INVALID_EXPRESSION_TYPE, typeof(Expression<Func<TModel, dynamic>>), expression.GetType()));
            }
        }

        /// <summary>
        /// Order by generic expression descending
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public IOrderableQueryResultSet OrderByDescending(Expression expression)
        {
            if (expression is Expression<Func<TModel, dynamic>> srt)
            {
                return this.OrderByDescending(srt);
            }
            else
            {
                throw new Exception(String.Format(ErrorMessages.INVALID_EXPRESSION_TYPE, typeof(Expression<Func<TModel, dynamic>>), expression.GetType()));
            }
        }

        /// <summary>
        /// Intersect the other result set - note this can only be of same type of set
        /// </summary>
        public IQueryResultSet Intersect(IQueryResultSet other)
        {
            if (other is MdmEntityResultSet<TModel> mq)
            {
                return this.Intersect(mq);
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(other), String.Format(ErrorMessages.ARGUMENT_INVALID_TYPE, typeof(MdmEntityResultSet<TModel>), other.GetType()));
            }
        }

        /// <summary>
        /// Intersect the other result set - note this can only be of same type of set
        /// </summary>
        public IQueryResultSet<TModel> Intersect(Expression<Func<TModel, bool>> filter)
        {
            return new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.Intersect(new ExpressionParameterRewriter<TModel, Entity, bool>(filter).Convert()), this.m_principal);
        }

        /// <summary>
        /// Intersect this set with <paramref name="other"/>
        /// </summary>
        public IQueryResultSet<TModel> Intersect(IQueryResultSet<TModel> other)
        {
            // We intersect the wrapped ones not these
            if (other is MdmEntityResultSet<TModel> otherMdm)
            {
                return new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.Intersect(otherMdm.m_wrappedResultSet), this.m_principal);
            }
            else if (other is IQueryResultSet<Entity> qre)
            {
                return new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.Intersect(qre), this.m_principal);
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.ARGUMENT_INCOMPATIBLE_TYPE, typeof(MdmEntityResultSet<TModel>), other.GetType()));
            }
        }

        /// <summary>
        /// Intersect the other result set - note this can only be of same type of set
        /// </summary>
        public IQueryResultSet<TModel> Union(Expression<Func<TModel, bool>> filter)
        {
            return new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.Union(new ExpressionParameterRewriter<TModel, Entity, bool>(filter).Convert()), this.m_principal);
        }

        /// <summary>
        /// Union the other result set - note this can only be of the same type of set
        /// </summary>
        public IQueryResultSet Union(IQueryResultSet other)
        {
            if (other is MdmEntityResultSet<TModel> mq)
            {
                return this.Union(mq);
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(other), String.Format(ErrorMessages.ARGUMENT_INVALID_TYPE, typeof(MdmEntityResultSet<TModel>), other.GetType()));
            }
        }

        /// <summary>
        /// Union this set with the <paramref name="other"/>
        /// </summary>
        public IQueryResultSet<TModel> Union(IQueryResultSet<TModel> other)
        {
            // We intersect the wrapped ones not these
            if (other is MdmEntityResultSet<TModel> otherMdm)
            {
                return new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.Union(otherMdm.m_wrappedResultSet), this.m_principal);
            }
            else if (other is IQueryResultSet<Entity> qre)
            {
                return new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.Union(qre), this.m_principal);
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.ARGUMENT_INCOMPATIBLE_TYPE, typeof(MdmEntityResultSet<TModel>), other.GetType()));
            }
        }

        /// <summary>
        /// Return objects in this collection which are of type <typeparamref name="TType"/>
        /// </summary>
        public IEnumerable<TType> OfType<TType>()
        {
            foreach (var itm in this)
            {
                if (itm is TType typ)
                {
                    yield return typ;
                }
            }
        }

        #endregion Non-Generic
    }
}
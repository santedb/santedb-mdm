﻿using SanteDB.Core;
using SanteDB.Core.i18n;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Security;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Model;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Security.Principal;
using System.Text;

namespace SanteDB.Persistence.MDM.Services.Resources
{
    /// <summary>
    /// A delay load query wrapper that synthesizes a series of mdm masters
    /// </summary>
    public class MdmEntityResultSet<TModel> : IQueryResultSet<TModel>, IOrderableQueryResultSet<TModel>
        where TModel : Entity, new()
    {
        // Wrapped result set
        private readonly IQueryResultSet<TModel> m_wrappedResultSet;

        // Principal consuming this result set
        private readonly IPrincipal m_principal;

        /// <summary>
        /// Creates a new mdm query result set
        /// </summary>
        /// <param name="wrappedResultSet">The result set as filtered ready to be converted to <see cref="EntityMaster{T}"/></param>
        internal MdmEntityResultSet(IQueryResultSet<TModel> wrappedResultSet, IPrincipal asPrincipal)
        {
            this.m_wrappedResultSet = wrappedResultSet;
            this.m_principal = asPrincipal;
        }

        /// <summary>
        /// True if this set has any results
        /// </summary>
        public bool Any() => this.m_wrappedResultSet.Any();

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
        public TModel First() => new EntityMaster<TModel>(this.m_wrappedResultSet.First()).Synthesize(this.m_principal);

        /// <summary>
        /// Get the first or default
        /// </summary>
        public TModel FirstOrDefault()
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

        /// <summary>
        /// Get the enumerator for this object
        /// </summary>
        public IEnumerator<TModel> GetEnumerator()
        {
            foreach (var itm in this.m_wrappedResultSet)
            {
                yield return new EntityMaster<TModel>(itm).Synthesize(this.m_principal);
            }
        }

        /// <summary>
        /// Retrieve a single object
        /// </summary>
        public TModel Single() => new EntityMaster<TModel>(this.m_wrappedResultSet.Single()).Synthesize(this.m_principal);

        /// <summary>
        /// Get single or default
        /// </summary>
        public TModel SingleOrDefault()
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
            return new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.Where(query), this.m_principal);
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
        /// Select specified data from entity master - note this will return only from the MASTER not a synthesized result
        /// </summary>
        public IEnumerable<TReturn> Select<TReturn>(Expression<Func<TModel, TReturn>> selector)
        {
            return this.m_wrappedResultSet.Select(selector);
        }

        /// <summary>
        /// Order by specified <paramref name="sortExpression"/>
        /// </summary>
        public IOrderableQueryResultSet<TModel> OrderBy(Expression<Func<TModel, dynamic>> sortExpression)
        {
            if (this.m_wrappedResultSet is IOrderableQueryResultSet<TModel> orderable)
            {
                return new MdmEntityResultSet<TModel>(orderable.OrderBy(sortExpression), this.m_principal);
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.NOT_SUPPORTED_IMPLEMENTATION, typeof(IOrderableQueryResultSet<TModel>)));
            }
        }

        /// <summary>
        /// Order result set by descending
        /// </summary>
        public IOrderableQueryResultSet<TModel> OrderByDescending(Expression<Func<TModel, dynamic>> sortExpression)
        {
            if (this.m_wrappedResultSet is IOrderableQueryResultSet<TModel> orderable)
            {
                return new MdmEntityResultSet<TModel>(orderable.OrderByDescending(sortExpression), this.m_principal);
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
            return this.m_wrappedResultSet.Select(selector);
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
            return new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.Intersect(filter), this.m_principal);
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
            else if (this.m_wrappedResultSet.GetType() == other.GetType())
            {
                return new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.Intersect(other), this.m_principal);
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
            return new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.Union(filter), this.m_principal);
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
            else if (this.m_wrappedResultSet.GetType() == other.GetType())
            {
                return new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.Union(other), this.m_principal);
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
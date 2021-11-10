using SanteDB.Core;
using SanteDB.Core.i18n;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Model;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace SanteDB.Persistence.MDM.Services.Resources
{
    /// <summary>
    /// A delay load query wrapper that synthesizes a series of mdm masters
    /// </summary>
    public class MdmEntityResultSet<TModel> : IQueryResultSet<EntityMaster<TModel>>, IQueryResultSet<IMdmMaster>
        where TModel : Entity, new()
    {
        // Wrapped result set
        private readonly IQueryResultSet<TModel> m_wrappedResultSet;

        /// <summary>
        /// Creates a new mdm query result set
        /// </summary>
        public MdmEntityResultSet(IQueryResultSet<TModel> wrappedResultSet)
        {
            this.m_wrappedResultSet = wrappedResultSet;
        }

        /// <summary>
        /// True if this set has any results
        /// </summary>
        public bool Any() => this.m_wrappedResultSet.Any();

        /// <summary>
        /// Get this result set as a stateful query
        /// </summary>
        public IQueryResultSet<EntityMaster<TModel>> AsStateful(Guid stateId) => new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.AsStateful(stateId));

        /// <summary>
        /// Gets the count of this query
        /// </summary>
        public int Count() => this.m_wrappedResultSet.Count();

        /// <summary>
        /// Get the first result set
        /// </summary>
        public EntityMaster<TModel> First() => new EntityMaster<TModel>(this.m_wrappedResultSet.First());

        /// <summary>
        /// Get the first or default
        /// </summary>
        public EntityMaster<TModel> FirstOrDefault()
        {
            var fd = this.m_wrappedResultSet.FirstOrDefault();
            if (fd == null)
            {
                return null;
            }
            else
            {
                return new EntityMaster<TModel>(fd);
            }
        }

        /// <summary>
        /// Get the enumerator for this object
        /// </summary>
        public IEnumerator<EntityMaster<TModel>> GetEnumerator()
        {
            foreach (var itm in this.m_wrappedResultSet)
            {
                yield return new EntityMaster<TModel>(itm);
            }
        }

        /// <summary>
        /// Intersect queries
        /// </summary>
        public IQueryResultSet<EntityMaster<TModel>> Intersect(Expression<Func<EntityMaster<TModel>, bool>> query)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Intersect two result sets
        /// </summary>
        public IQueryResultSet<EntityMaster<TModel>> Intersect(IQueryResultSet<EntityMaster<TModel>> other)
        {
            if (other is MdmEntityResultSet<TModel> ot)
            {
                return new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.Intersect(ot.m_wrappedResultSet));
            }
            else
            {
                throw new ArgumentException(nameof(other), String.Format(ErrorMessages.ARGUMENT_INVALID_TYPE, typeof(MdmEntityResultSet<TModel>), other.GetType()));
            }
        }

        /// <summary>
        /// Perform an intersect
        /// </summary>
        public IQueryResultSet<IMdmMaster> Intersect(Expression<Func<IMdmMaster, bool>> query)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Intersect with another
        /// </summary>
        IQueryResultSet<IMdmMaster> IQueryResultSet<IMdmMaster>.Intersect(IQueryResultSet<IMdmMaster> other) => this.Intersect(other as IQueryResultSet<EntityMaster<TModel>>) as MdmEntityResultSet<TModel>;

        /// <summary>
        /// Order the result set
        /// </summary>
        public IQueryResultSet<EntityMaster<TModel>> OrderBy(Expression<Func<EntityMaster<TModel>, dynamic>> sortExpression)
        {
            var newSort = new ExpressionConvertVisitor<EntityMaster<TModel>, TModel, dynamic>(sortExpression).Convert();
            return new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.OrderBy(newSort));
        }

        /// <summary>
        /// Cannot order by this verison of the expression
        /// </summary>
        IQueryResultSet<IMdmMaster> IQueryResultSet<IMdmMaster>.OrderBy(Expression<Func<IMdmMaster, dynamic>> sortExpression)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Order the result set descending
        /// </summary>
        public IQueryResultSet<EntityMaster<TModel>> OrderByDescending(Expression<Func<EntityMaster<TModel>, dynamic>> sortExpression)
        {
            var newSort = new ExpressionConvertVisitor<EntityMaster<TModel>, TModel, dynamic>(sortExpression).Convert();
            return new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.OrderByDescending(newSort));
        }

        /// <summary>
        /// Cannot order by this version of the expression
        /// </summary>
        IQueryResultSet<IMdmMaster> IQueryResultSet<IMdmMaster>.OrderByDescending(Expression<Func<IMdmMaster, dynamic>> sortExpression)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Retrieve a single object
        /// </summary>
        public EntityMaster<TModel> Single() => new EntityMaster<TModel>(this.m_wrappedResultSet.Single());

        /// <summary>
        /// Get single or default
        /// </summary>
        public EntityMaster<TModel> SingleOrDefault()
        {
            var sd = this.m_wrappedResultSet.SingleOrDefault();
            if (sd == null)
            {
                return null;
            }
            else
            {
                return new EntityMaster<TModel>(sd);
            }
        }

        /// <summary>
        /// Skip the number of results
        /// </summary>
        public IQueryResultSet<EntityMaster<TModel>> Skip(int count) => new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.Skip(count));

        /// <summary>
        /// Take the number of results
        /// </summary>
        public IQueryResultSet<EntityMaster<TModel>> Take(int count) => new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.Take(count));

        /// <summary>
        /// Union with another result query
        /// </summary>
        public IQueryResultSet<EntityMaster<TModel>> Union(Expression<Func<EntityMaster<TModel>, bool>> query)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Union with another result set
        /// </summary>
        public IQueryResultSet<EntityMaster<TModel>> Union(IQueryResultSet<EntityMaster<TModel>> other)
        {
            if (other is MdmEntityResultSet<TModel> ot)
            {
                return new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.Union(ot.m_wrappedResultSet));
            }
            else
            {
                throw new ArgumentException(nameof(other), String.Format(ErrorMessages.ARGUMENT_INVALID_TYPE, typeof(MdmEntityResultSet<TModel>), other.GetType()));
            }
        }

        /// <summary>
        /// Cannot union
        /// </summary>
        IQueryResultSet<IMdmMaster> IQueryResultSet<IMdmMaster>.Union(Expression<Func<IMdmMaster, bool>> query)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Union with another
        /// </summary>
        IQueryResultSet<IMdmMaster> IQueryResultSet<IMdmMaster>.Union(IQueryResultSet<IMdmMaster> other) => this.Union(other as IQueryResultSet<EntityMaster<TModel>>) as MdmEntityResultSet<TModel>;

        /// <summary>
        /// Filter according to a where clause
        /// </summary>
        public IQueryResultSet<EntityMaster<TModel>> Where(Expression<Func<EntityMaster<TModel>, bool>> query)
        {
            // TODO: Strip the query over to entity
            var newQuery = new ExpressionConvertVisitor<EntityMaster<TModel>, TModel, bool>(query).Convert();
            return new MdmEntityResultSet<TModel>(this.m_wrappedResultSet.Where(newQuery));
        }

        /// <summary>
        /// Returns the filtered expression on query
        /// </summary>
        public IQueryResultSet Where(Expression query)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Cannot filter by this version of the interface
        /// </summary>
        IQueryResultSet<IMdmMaster> IQueryResultSet<IMdmMaster>.Where(Expression<Func<IMdmMaster, bool>> query)
        {
            throw new NotImplementedException();
        }

        #region Non-Generic

        IQueryResultSet IQueryResultSet.AsStateful(Guid stateId) => this.AsStateful(stateId);

        IQueryResultSet<IMdmMaster> IQueryResultSet<IMdmMaster>.AsStateful(Guid stateId) => this.AsStateful(stateId) as MdmEntityResultSet<TModel>;

        object IQueryResultSet.First() => this.First();

        IMdmMaster IQueryResultSet<IMdmMaster>.First() => this.First();

        object IQueryResultSet.FirstOrDefault() => this.FirstOrDefault();

        IMdmMaster IQueryResultSet<IMdmMaster>.FirstOrDefault() => this.FirstOrDefault();

        IEnumerator IEnumerable.GetEnumerator() => this.GetEnumerator();

        IEnumerator<IMdmMaster> IEnumerable<IMdmMaster>.GetEnumerator() => this.GetEnumerator();

        object IQueryResultSet.Single() => this.Single();

        IMdmMaster IQueryResultSet<IMdmMaster>.Single() => this.Single();

        object IQueryResultSet.SingleOrDefault() => this.SingleOrDefault();

        IMdmMaster IQueryResultSet<IMdmMaster>.SingleOrDefault() => this.SingleOrDefault();

        IQueryResultSet IQueryResultSet.Skip(int count) => this.Skip(count);

        IQueryResultSet<IMdmMaster> IQueryResultSet<IMdmMaster>.Skip(int count) => this.Skip(count) as MdmEntityResultSet<TModel>;

        IQueryResultSet IQueryResultSet.Take(int count) => this.Take(count);

        IQueryResultSet<IMdmMaster> IQueryResultSet<IMdmMaster>.Take(int count) => this.Take(count) as MdmEntityResultSet<TModel>;

        /// <summary>
        /// Select specified data from entity master
        /// </summary>
        public IEnumerable<TReturn> Select<TReturn>(Expression<Func<EntityMaster<TModel>, TReturn>> selector)
        {
            var newSelector = new ExpressionConvertVisitor<EntityMaster<TModel>, TModel, TReturn>(selector).Convert();
            return this.m_wrappedResultSet.Select(newSelector);
        }

        /// <summary>
        /// Select from a IMaster
        /// </summary>
        public IEnumerable<TReturn> Select<TReturn>(Expression<Func<IMdmMaster, TReturn>> selector)
        {
            throw new NotSupportedException();
        }

        #endregion Non-Generic
    }
}
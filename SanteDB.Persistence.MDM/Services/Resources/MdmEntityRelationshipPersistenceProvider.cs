using SanteDB.Core.Event;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Model;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Security.Principal;
using System.Text;

namespace SanteDB.Persistence.MDM.Services.Resources
{
    /// <summary>
    /// Relationship persistence provider passed through to underlying types
    /// </summary>
    internal class MdmEntityRelationshipPersistenceProvider : IRepositoryService<EntityRelationshipMaster>, IDataPersistenceService<EntityRelationshipMaster>
    {
        private readonly IRepositoryService<EntityRelationship> m_repositoryService;
        private readonly IDataPersistenceService<EntityRelationship> m_persistenceService;

        /// <summary>
        /// DI ctor
        /// </summary>
        public MdmEntityRelationshipPersistenceProvider(IRepositoryService<EntityRelationship> repositoryService, IDataPersistenceService<EntityRelationship> dataPersistenceService)
        {
            this.m_repositoryService = repositoryService;
            this.m_persistenceService = dataPersistenceService;
        }

        /// <inheritdoc/>
        public string ServiceName => "MDM Relationship Reroute Provider";


        /// <inheritdoc/>
        public event EventHandler<DataPersistedEventArgs<EntityRelationshipMaster>> Inserted;
        /// <inheritdoc/>
        public event EventHandler<DataPersistingEventArgs<EntityRelationshipMaster>> Inserting;
        /// <inheritdoc/>
        public event EventHandler<DataPersistedEventArgs<EntityRelationshipMaster>> Updated;
        /// <inheritdoc/>
        public event EventHandler<DataPersistingEventArgs<EntityRelationshipMaster>> Updating;
        /// <inheritdoc/>
        public event EventHandler<DataPersistedEventArgs<EntityRelationshipMaster>> Deleted;
        /// <inheritdoc/>
        public event EventHandler<DataPersistingEventArgs<EntityRelationshipMaster>> Deleting;
        /// <inheritdoc/>
        public event EventHandler<QueryResultEventArgs<EntityRelationshipMaster>> Queried;
        /// <inheritdoc/>
        public event EventHandler<QueryRequestEventArgs<EntityRelationshipMaster>> Querying;
        /// <inheritdoc/>
        public event EventHandler<DataRetrievingEventArgs<EntityRelationshipMaster>> Retrieving;
        /// <inheritdoc/>
        public event EventHandler<DataRetrievedEventArgs<EntityRelationshipMaster>> Retrieved;

        private Expression<Func<EntityRelationship, bool>> ConvertExpression(Expression<Func<EntityRelationshipMaster, bool>> query) => new ExpressionParameterRewriter<EntityRelationshipMaster, EntityRelationship, bool>(query).Convert();

        /// <inheritdoc/>
        [Obsolete]
        public long Count(Expression<Func<EntityRelationshipMaster, bool>> query, IPrincipal authContext = null)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Delete the specified object
        /// </summary>
        public EntityRelationshipMaster Delete(Guid key) => this.m_repositoryService.Delete(key).Convert<EntityRelationshipMaster>();

        /// <inheritdoc/>
        public EntityRelationshipMaster Delete(Guid key, TransactionMode transactionMode, IPrincipal principal) => this.m_persistenceService.Delete(key, transactionMode, principal).Convert<EntityRelationshipMaster>();

        /// <inheritdoc/>
        public IQueryResultSet<EntityRelationshipMaster> Find(Expression<Func<EntityRelationshipMaster, bool>> query) => new TransformQueryResultSet<EntityRelationship, EntityRelationshipMaster>(this.m_repositoryService.Find(this.ConvertExpression(query)), o => o.Convert<EntityRelationshipMaster>());

        /// <inheritdoc/>
        public IEnumerable<EntityRelationshipMaster> Find(Expression<Func<EntityRelationshipMaster, bool>> query, int offset, int? count, out int totalResults, params ModelSort<EntityRelationshipMaster>[] orderBy)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public EntityRelationshipMaster Get(Guid key) => this.m_repositoryService.Get(key).Convert<EntityRelationshipMaster>();

        /// <inheritdoc/>
        public EntityRelationshipMaster Get(Guid key, Guid versionKey) => this.Get(key);

        /// <inheritdoc/>
        public EntityRelationshipMaster Get(Guid key, Guid? versionKey, IPrincipal principal) => this.m_persistenceService.Get(key, versionKey, principal).Convert<EntityRelationshipMaster>();

        /// <inheritdoc/>
        public EntityRelationshipMaster Insert(EntityRelationshipMaster data) => this.m_repositoryService.Insert(data.ToEntityRelationship()).Convert<EntityRelationshipMaster>();

        /// <inheritdoc/>
        public EntityRelationshipMaster Insert(EntityRelationshipMaster data, TransactionMode transactionMode, IPrincipal principal) => this.m_persistenceService.Insert(data.ToEntityRelationship(), transactionMode, principal).Convert<EntityRelationshipMaster>();

        /// <inheritdoc/>
        public IQueryResultSet<EntityRelationshipMaster> Query(Expression<Func<EntityRelationshipMaster, bool>> query, IPrincipal principal) => new TransformQueryResultSet<EntityRelationship, EntityRelationshipMaster>(this.m_persistenceService.Query(this.ConvertExpression(query), principal), o => o.Convert<EntityRelationshipMaster>());

        /// <inheritdoc/>
        [Obsolete]
        public IEnumerable<EntityRelationshipMaster> Query(Expression<Func<EntityRelationshipMaster, bool>> query, int offset, int? count, out int totalResults, IPrincipal principal, params ModelSort<EntityRelationshipMaster>[] orderBy)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public EntityRelationshipMaster Save(EntityRelationshipMaster data) => this.m_repositoryService.Save(data.ToEntityRelationship()).Convert<EntityRelationshipMaster>();

        /// <inheritdoc/>
        public EntityRelationshipMaster Update(EntityRelationshipMaster data, TransactionMode transactionMode, IPrincipal principal) => this.m_persistenceService.Update(data.ToEntityRelationship(), transactionMode, principal).Convert<EntityRelationshipMaster>();

    }
}

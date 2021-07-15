using SanteDB.Core;
using SanteDB.Core.Configuration;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Event;
using SanteDB.Core.Exceptions;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Security;
using SanteDB.Core.Security.Claims;
using SanteDB.Core.Security.Principal;
using SanteDB.Core.Security.Services;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Principal;
using System.Text;

namespace SanteDB.Persistence.MDM.Services.Resources
{
    /// <summary>
    /// Represents a class that only intercepts events from the repository layer
    /// </summary>
    public class MdmResourceHandler<TModel> : IDisposable, IMdmResourceHandler
        where TModel : IdentifiedData, new()
    {

        // Policy enforcement service
        private IPolicyEnforcementService m_policyEnforcement;

        // Tracer
        private Tracer m_traceSource = new Tracer(MdmConstants.TraceSourceName);

        // The notification repository
        private INotifyRepositoryService<TModel> m_notifyRepository;

        // Batch repository
        private IDataPersistenceService<Bundle> m_batchRepository;

        // Data manager
        private MdmDataManager<TModel> m_dataManager;

        /// <summary>
        /// Resource listener
        /// </summary>
        public MdmResourceHandler()
        {
            // Register the master 
            this.m_dataManager = MdmDataManagerFactory.GetDataManager<TModel>();
            this.m_policyEnforcement = ApplicationServiceContext.Current.GetService<IPolicyEnforcementService>();
            this.m_batchRepository = ApplicationServiceContext.Current.GetService<IDataPersistenceService<Bundle>>();

            // Validate the match configuration exists
            var matchConfigService = ApplicationServiceContext.Current.GetService<IRecordMatchingConfigurationService>();

            this.m_notifyRepository = ApplicationServiceContext.Current.GetService<IRepositoryService<TModel>>() as INotifyRepositoryService<TModel>;
            if (this.m_notifyRepository == null)
                throw new InvalidOperationException($"Could not find repository service for {typeof(TModel)}");

            // Subscribe
            this.m_notifyRepository.Inserting += this.OnPrePersistenceValidate;
            this.m_notifyRepository.Saving += this.OnPrePersistenceValidate;
            this.m_notifyRepository.Obsoleting += this.OnPrePersistenceValidate;
            this.m_notifyRepository.Inserting += this.OnInserting;
            this.m_notifyRepository.Saving += this.OnSaving;
            this.m_notifyRepository.Obsoleting += this.OnObsoleting;
            this.m_notifyRepository.Retrieved += this.OnRetrieved;
            this.m_notifyRepository.Retrieving += this.OnRetrieving;
            this.m_notifyRepository.Querying += this.OnQuerying;

        }

        /// <summary>
        /// Validate retrieval permission
        /// </summary>
        internal virtual void OnRetrieved(object sender, DataRetrievedEventArgs<TModel> e)
        {
            if (!this.m_dataManager.IsMaster(e.Data) &&
                e.Principal != AuthenticationContext.SystemPrincipal
                && !this.m_dataManager.IsOwner(e.Data, e.Principal))
            {
                this.m_policyEnforcement.Demand(MdmPermissionPolicyIdentifiers.ReadMdmLocals, e.Principal);
            }
        }

        /// <summary>
        /// Called before the repository is querying
        /// </summary>
        internal virtual void OnQuerying(object sender, QueryRequestEventArgs<TModel> e)
        {
            // system does whatever tf they want
            var query = new NameValueCollection(QueryExpressionBuilder.BuildQuery<TModel>(e.Query).ToArray());

            // They are specifically asking for records
            if (query.TryGetValue("tag[$mdm.type]", out List<String> mdmFilter))
            {
                if (mdmFilter.Contains("L"))
                {
                    this.m_policyEnforcement.Demand(MdmPermissionPolicyIdentifiers.ReadMdmLocals, e.Principal);
                    mdmFilter.Remove("L"); // Just allow the repo to be queried
                }
                else if (mdmFilter.Contains("M")) // pure master query 
                {
                    throw new NotImplementedException("Msater queries not supported yet");
                }
            }
            else
            {
                var localQuery = new NameValueCollection(query.ToDictionary(o => $"relationship[{MdmConstants.MasterRecordRelationship}].source@{typeof(TModel).Name}.{o.Key}", o => o.Value));
                query.Add("classConcept", MdmConstants.MasterRecordClassification.ToString());
                e.Cancel = true; // We want to cancel the callers query
                
                // We are wrapping an entity, so we query entity masters
                e.Results = this.m_dataManager.MdmQuery(query, localQuery, e.QueryId, e.Offset, e.Count, out int tr).Select(o => o.GetMaster(e.Principal)).OfType<TModel>();
                e.TotalResults = tr;
            }
        }

        /// <summary>
        /// Retrieving a specific object
        /// </summary>
        internal virtual void OnRetrieving(object sender, DataRetrievingEventArgs<TModel> e)
        {
            if (this.m_dataManager.IsMaster(e.Id.Value)) // object is a master 
            {
                e.Cancel = true;
                e.Result = (TModel)this.m_dataManager.MdmGet(e.Id.Value).GetMaster(e.Principal);
            }
        }

        /// <summary>
        /// Fired before a record is obsoleted
        /// </summary>
        /// <remarks>We don't want a MASTER record to be obsoleted under any condition. MASTER records require special permission to 
        /// obsolete and also require that all LOCAL records be either re-assigned or obsoleted as well.</remarks>
        internal virtual void OnObsoleting(object sender, DataPersistingEventArgs<TModel> e)
        {
            if (sender is Bundle bundle)
            {
                var obsoleteInstructions = this.m_dataManager.MdmTxObsolete(e.Data, bundle.Item);
                bundle.AddRange(obsoleteInstructions);
            }
            else
            {
                var obsoleteInstructions = this.m_dataManager.MdmTxObsolete(e.Data, null);
                this.m_batchRepository.Update(new Bundle(obsoleteInstructions), TransactionMode.Commit, e.Principal);
            }

            e.Cancel = true;
            e.Success = true;
        }

        /// <summary>
        /// Fired prior to saving being performed
        /// </summary>
        internal virtual void OnSaving(object sender, DataPersistingEventArgs<TModel> e)
        {
            
            try
            {
                // Prevent duplicate processing
                if (e.Data is ITaggable taggable)
                {
                    if (!String.IsNullOrEmpty(taggable.GetTag(MdmConstants.MdmProcessedTag)))
                    {
                        return;
                    }
                    else
                    {
                        taggable.AddTag(MdmConstants.MdmProcessedTag, "true");
                    }
                }

                // Is the sender a bundle?
                if (sender is Bundle bundle)
                {
                    var transactionItems = this.PrepareTransaction(e.Data, bundle.Item);
                    bundle.Item.InsertRange(bundle.Item.FindIndex(o => o.Key == e.Data.Key), transactionItems.Where(o => o != e.Data));
                }
                else
                {
                    var transactionItems = this.PrepareTransaction(e.Data, null);
                    bundle = new Bundle(transactionItems);
                    var bre = ApplicationServiceContext.Current.GetBusinessRuleService(typeof(Bundle));
                    bundle = (Bundle)bre?.BeforeUpdate(bundle) ?? bundle;
                    bundle = this.m_batchRepository.Update(bundle, TransactionMode.Commit, e.Principal);
                    bundle = (Bundle)bre?.AfterUpdate(bundle) ?? bundle;
                    e.Data = bundle.Item.Find(o => o.Key == e.Data.Key) as TModel; // copy to get key data

                }

                e.Cancel = true;
                e.Success = true;
            }
            catch (Exception ex)
            {
                throw new MdmException(e.Data, "Error Executing INSERT trigger", ex);
            }
        }

        /// <summary>
        /// Prepare transaction bundle from inbound arguments
        /// </summary>
        internal virtual IEnumerable<IdentifiedData> PrepareTransaction(TModel data, IEnumerable<IdentifiedData> context)
        {

            if (context == null)
            {
                context = new List<IdentifiedData>();
            }

            // The data being updated as a ROT - 
            if (this.m_dataManager.IsRecordOfTruth(data))
            {
                return this.m_dataManager.MdmTxSaveRecordOfTruth(data, context);
            }
            else
            {
                return this.m_dataManager.MdmTxSaveLocal(data, context);
            }

        }

        /// <summary>
        /// Perform insert
        /// </summary>
        internal virtual void OnInserting(object sender, DataPersistingEventArgs<TModel> e)
        {

            try
            {
                // Prevent duplicate processing
                if (e.Data is ITaggable taggable)
                {
                    if (!String.IsNullOrEmpty(taggable.GetTag(MdmConstants.MdmProcessedTag)))
                    {
                        return;
                    }
                    else
                    {
                        taggable.AddTag(MdmConstants.MdmProcessedTag, "true");
                    }
                }

                // Is the sender a bundle?
                if (sender is Bundle bundle)
                {
                    bundle.Item = this.PrepareTransaction(e.Data, bundle.Item).ToList();
                }
                else
                {
                    var transactionItems = this.PrepareTransaction(e.Data, null);
                    bundle = new Bundle(transactionItems);
                    var bre = ApplicationServiceContext.Current.GetBusinessRuleService(typeof(Bundle));
                    bundle = (Bundle)bre?.BeforeInsert(bundle) ?? bundle;
                    bundle = this.m_batchRepository.Insert(bundle, TransactionMode.Commit, e.Principal);
                    bundle = (Bundle)bre?.AfterInsert(bundle) ?? bundle;

                    e.Data = bundle.Item.Find(o => o.Key == e.Data.Key) as TModel; // copy to get key data
                }
                e.Cancel = true;
                e.Success = true;
            }
            catch (Exception ex)
            {
                throw new MdmException(e.Data, "Error Executing INSERT trigger", ex);
            }
        }

        /// <summary>
        /// Called prior to persisting 
        /// </summary>
        /// <remarks>This method seeks to establish whether the caller is attempting to 
        /// create/update a master</remarks>
        internal virtual void OnPrePersistenceValidate(object sender, DataPersistingEventArgs<TModel> e)
        {

            var store = e.Data;
            // Is the existing object a master?
            if (this.m_dataManager.IsMaster(e.Data))
            {
                store = this.m_dataManager.GetLocalFor(e.Data.Key.GetValueOrDefault(), e.Principal); // Get a local for this object
                if (store == null)
                {
                    store = this.m_dataManager.CreateLocalFor(e.Data);
                }
                else
                {
                    // Copy the changed data from the inbound to the new local
                    store.SemanticCopy(e.Data);
                    store.SemanticCopy(e.Data);
                }

                if (store is IVersionedEntity versioned)
                {
                    versioned.VersionSequence = null;
                    versioned.VersionKey = null;
                }
                
            }
            else if (!store.Key.HasValue)
            {
                store.Key = Guid.NewGuid(); // Ensure that we have a key for the object.
            }

            store.StripAssociatedItemSources();

            // Is this a ROT?
            if (this.m_dataManager.IsRecordOfTruth(e.Data))
            {
                this.m_policyEnforcement.Demand(MdmPermissionPolicyIdentifiers.EstablishRecordOfTruth);
                store = this.m_dataManager.PromoteRecordOfTruth(store);
            }

            // Rewrite any relationships we need to
            if (sender is Bundle bundle)
            {
                if (e.Data != store) // storage has changed 
                {
                    bundle.Item.Insert(bundle.Item.IndexOf(e.Data), store);
                    bundle.Item.Remove(e.Data); // Remove 
                }
                bundle.Item.AddRange(this.m_dataManager.ExtractRelationships(store).OfType<IdentifiedData>());
                this.m_dataManager.RefactorRelationships(bundle.Item, e.Data.Key.Value, store.Key.Value);
            }
            else
            {
                this.m_dataManager.RefactorRelationships(new List<IdentifiedData>() { store }, e.Data.Key.Value, store.Key.Value);
            }

            e.Data = store;
        }

        /// <summary>
        /// Dispose of this object
        /// </summary>
        public virtual void Dispose()
        {
            if (this.m_notifyRepository != null)
            {
                this.m_notifyRepository.Inserting -= this.OnPrePersistenceValidate;
                this.m_notifyRepository.Saving -= this.OnPrePersistenceValidate;
                this.m_notifyRepository.Inserting -= this.OnInserting;
                this.m_notifyRepository.Saving -= this.OnSaving;
                this.m_notifyRepository.Retrieving -= this.OnRetrieving;
                this.m_notifyRepository.Obsoleting -= this.OnObsoleting;
                this.m_notifyRepository.Querying -= this.OnQuerying;
            }
        }

        /// <summary>
        /// Generic version of on pre validate
        /// </summary>
        void IMdmResourceHandler.OnPrePersistenceValidate(object sender, object args) => this.OnPrePersistenceValidate(sender, (DataPersistingEventArgs<TModel>)args);

        /// <summary>B
        /// On inserting generic version
        /// </summary>
        void IMdmResourceHandler.OnInserting(object sender, object args) => this.OnInserting(sender, (DataPersistingEventArgs<TModel>)args);

        /// <summary>
        /// On saving non-generic version
        /// </summary>
        void IMdmResourceHandler.OnSaving(object sender, object args) => this.OnSaving(sender, (DataPersistingEventArgs<TModel>)args);
        
        /// <summary>
        /// On obsoleting non-generic
        /// </summary>
        void IMdmResourceHandler.OnObsoleting(object sender, object args) => this.OnObsoleting(sender, (DataPersistingEventArgs<TModel>)args);
    }
}

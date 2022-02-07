/*
 * Copyright (C) 2021 - 2022, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2021-10-29
 */
using SanteDB.Core;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Event;
using SanteDB.Core.Matching;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Acts;
using SanteDB.Core.Model.Attributes;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Model.Constants;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Security;
using SanteDB.Core.Security.Services;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Exceptions;
using SanteDB.Persistence.MDM.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Security.Principal;
using System.Xml.Serialization;

namespace SanteDB.Persistence.MDM.Services.Resources
{
    /// <summary>
    /// Represents a class that only intercepts events from the repository layer
    /// </summary>
    public class MdmResourceHandler<TModel> : IDisposable, IMdmResourceHandler
        where TModel : IdentifiedData, new()
    {
        // Class concept key
        private Guid[] m_classConceptKey;

        // Policy enforcement service
        private IPolicyEnforcementService m_policyEnforcement;

        // Tracer
        private Tracer m_traceSource = new Tracer(MdmConstants.TraceSourceName);

        // The notification repository
        private INotifyRepositoryService<TModel> m_notifyRepository;

        // Batch repository
        private IDataPersistenceService<Bundle> m_batchRepository;

        // Ad-hoc cache
        private readonly IAdhocCacheService m_adhocCache;

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
            this.m_adhocCache = ApplicationServiceContext.Current.GetService<IAdhocCacheService>();

            // Validate the match configuration exists
            var matchConfigService = ApplicationServiceContext.Current.GetService<IRecordMatchingConfigurationService>();

            this.m_notifyRepository = ApplicationServiceContext.Current.GetService<IRepositoryService<TModel>>() as INotifyRepositoryService<TModel>;
            if (this.m_notifyRepository == null)
                throw new InvalidOperationException($"Could not find repository service for {typeof(TModel)}");

            this.m_classConceptKey = typeof(TModel).GetCustomAttributes<ClassConceptKeyAttribute>(false).Select(o => Guid.Parse(o.ClassConcept)).ToArray();

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

            // Bind down the repository services
            // TODO: Determine if this level of interception is required - this only impacts when (for example) IRepositoryService<Patient>
            // is bound to MDM and someone calls IRepositoryService<Person> - without it calls to the former will return MDM based resources
            // whereas calls to the latter will return raw non-MDM resources (even if they are patients - they will be local, non-MDM patients).
            var baseType = typeof(TModel).BaseType;
            while (typeof(Entity).IsAssignableFrom(baseType) || typeof(Act).IsAssignableFrom(baseType))
            {
                var repoType = typeof(INotifyRepositoryService<>).MakeGenericType(baseType);
                var repoInstance = ApplicationServiceContext.Current.GetService(repoType);
                if (repoInstance == null)
                {
                    break;
                }

                // Hand off reflection notifcation
                var eventHandler = repoType.GetEvent("Queried");
                var parmType = typeof(QueryResultEventArgs<>).MakeGenericType(baseType);
                var parameter = Expression.Parameter(parmType);
                var dataAccess = Expression.MakeMemberAccess(parameter, parmType.GetProperty("Results"));
                var principalAccess = Expression.MakeMemberAccess(parameter, parmType.GetProperty("Principal"));
                var methodInfo = this.GetType().GetGenericMethod(nameof(OnGenericQueried), new Type[] { baseType }, new Type[] { typeof(IEnumerable<>).MakeGenericType(baseType), typeof(IPrincipal) });
                var lambdaMethod = typeof(Expression).GetGenericMethod(nameof(Expression.Lambda), new Type[] { eventHandler.EventHandlerType }, new Type[] { typeof(Expression), typeof(ParameterExpression[]) });
                var lambdaAccess = lambdaMethod.Invoke(null, new object[] { Expression.Assign(dataAccess, Expression.Call(Expression.Constant(this), (MethodInfo)methodInfo, dataAccess, principalAccess)), new ParameterExpression[] { Expression.Parameter(typeof(Object)), parameter } }) as LambdaExpression;
                eventHandler.AddEventHandler(repoInstance, lambdaAccess.Compile());
                //var lamdaAccess = Expression.Lambda(Expression.Call(Expression.Constant(this), this.GetType().GetMethod(nameof(OnGenericQueried))), dataAccess), Expression.Parameter(typeof(Object)), parameter);
                // Continue down base types
                baseType = baseType.BaseType;
            }
        }

        /// <summary>
        /// Handles when a generic repository (above the repository this is subscribed to) is queried
        /// </summary>
        public IEnumerable<TReturn> OnGenericQueried<TReturn>(IEnumerable<TReturn> results, IPrincipal principal)
            where TReturn : IdentifiedData
        {
            return results.Select(o =>
            {
                // It is a type controlled by this handler - so we want to ensure we return the master rather than a local
                if (o is TModel tmodel && !this.m_dataManager.IsMaster(tmodel))
                {
                    return this.m_dataManager.GetMasterContainerForMasterEntity(tmodel.Key.Value).Synthesize(principal);
                }
                // It is a type which is classified as a master and has a type concept
                else if (o is IHasClassConcept ihcc && ihcc.ClassConceptKey == MdmConstants.MasterRecordClassification &&
                    o is IHasTypeConcept ihtc && this.m_classConceptKey.Contains(ihtc.TypeConceptKey.GetValueOrDefault()))
                {
                    return this.m_dataManager.GetMasterContainerForMasterEntity(o.Key.Value).Synthesize(principal);
                }
                else
                {
                    return o;
                }
            }).OfType<TReturn>();
        }

        /// <summary>
        /// Validate retrieval permission
        /// </summary>
        internal virtual void OnRetrieved(object sender, DataRetrievedEventArgs<TModel> e)
        {
            if (e.Data != null &&
                !this.m_dataManager.IsMaster(e.Data) &&
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
                    throw new NotImplementedException("Master queries not supported yet");
                }
            }
            else
            {

                // Determine the level of casting required
                var castStack = new Stack<Type>();
                var ctype = typeof(TModel);
                while(ctype != typeof(IdentifiedData))
                {
                    if (ctype.GetCustomAttribute<XmlRootAttribute>() != null)
                        castStack.Push(ctype);
                    ctype = ctype.BaseType;
                }
                // Iterate through the query and determine the level of casting
                while(castStack.Count > 0)
                {
                    ctype = castStack.Pop();
                    try
                    {
                        query.Select(o => QueryExpressionParser.BuildPropertySelector(ctype, o.Key)).ToArray();
                        break;
                    }
                    catch { }
                }
                
                var localQuery = new NameValueCollection(query.ToDictionary(o => $"relationship[{MdmConstants.MasterRecordRelationship}].source@{ctype.Name}.{o.Key}", o => new List<string>(o.Value)));
                if (!query.TryGetValue("statusConcept", out _))
                {
                    localQuery.Add($"relationship[{MdmConstants.MasterRecordRelationship}].source@{ctype.Name}.statusConcept", StatusKeys.ActiveStates.Select(o => o.ToString()));
                    localQuery.Add("statusConcept", StatusKeys.ActiveStates.Select(o => o.ToString()));
                }
                else
                {
                    localQuery.Add("statusConcept", query["statusConcept"]);
                }
                localQuery.Add("obsoletionTime", "null");

                e.Cancel = true; // We want to cancel the callers query

                //// Trim the local query
                foreach (var itm in query.ToArray())
                {
                    var keyField = itm.Key.Split('[', '.');
                    switch (keyField[0])
                    {
                        case "identifier":
                            break;
                        case "id":
                            itm.Value.RemoveAll(o => o == "!null" || o.StartsWith("!"));
                            if(itm.Value.Any())
                            {
                                break;
                            }
                            else
                            {
                                query.Remove(itm.Key);
                            }
                            break;
                        default:
                            query.Remove(itm.Key);
                            break;
                    }
                }

                if (query.Any())
                {
                    query.Add("classConcept", MdmConstants.MasterRecordClassification.ToString());
                    if (localQuery.TryGetValue("statusConcept", out var status)) {
                        query.Add("statusConcept", status);
                    }
                }

                // We are wrapping an entity, so we query entity masters
                e.Results = this.m_dataManager.MdmQuery(query, localQuery, e.QueryId, e.Offset, e.Count, out int tr, e.OrderBy).Select(o => o.Synthesize(e.Principal)).OfType<TModel>();
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
                e.Result = (TModel)this.m_dataManager.MdmGet(e.Id.Value).Synthesize(e.Principal);
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
                e.Data.BatchOperation = Core.Model.DataTypes.BatchOperationType.InsertOrUpdate;

                if (sender is Bundle bundle)
                {
                    // need to create a new list to avoid the collection being modified during enumeration
                    var transactionItems = new List<IdentifiedData>(this.PrepareTransaction(e.Data, bundle.Item));
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
                throw new MdmException(e.Data, "Error Executing UPDATE trigger", ex);
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
                e.Data.BatchOperation = Core.Model.DataTypes.BatchOperationType.InsertOrUpdate;
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
            var originalKey = e.Data.Key ?? (Guid?)Guid.NewGuid();
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
                    // Backup the core properties
                    var oldStore = store.Clone() as TModel;
                    store = e.Data.Clone() as TModel;
                    store.Key = oldStore.Key;
                    if(store is IHasState state && oldStore is IHasState oldState)
                    {
                        state.StatusConceptKey = oldState.StatusConceptKey;
                    }
                    if(store is Entity storeEnt && oldStore is Entity oldEnt)
                    {
                        storeEnt.DeterminerConceptKey = oldEnt.DeterminerConceptKey;
                        storeEnt.ClassConceptKey = oldEnt.ClassConceptKey;
                    }
                }

                // So - this is complex but here is a description of why we do the next line of code:
                //  Basically we never want to explicitly let the client send us an EntityRelationshipMaster on the
                //  service instance. So we need to select back out of any provided relationships the
                //  relationship masters to only those which apply to our object rather than the
                //  redirected relationships
                if (store is IHasRelationships irelationships)
                {
                    // Now re-process the relationships
                    foreach (var itm in irelationships.Relationships.ToArray())
                    {
                        if (itm is IMdmRedirectedRelationship imdmrdr)
                        {
                            if (imdmrdr.OriginalTargetKey.HasValue && imdmrdr.OriginalHolderKey != store.Key)
                            {
                                irelationships.RemoveRelationship(imdmrdr);
                            }
                            else if (imdmrdr.OriginalTargetKey.HasValue && imdmrdr.OriginalTargetKey != imdmrdr.TargetEntityKey) // the pointer is to another MDM master - so fix it
                            {
                                imdmrdr.TargetEntityKey = imdmrdr.OriginalTargetKey;
                            }
                            else
                            {
                                imdmrdr.SourceEntityKey = imdmrdr.OriginalHolderKey ?? store.Key;
                                imdmrdr.TargetEntityKey = imdmrdr.OriginalTargetKey ?? imdmrdr.TargetEntityKey;
                            }
                        }
                        else if (itm.SourceEntityKey.HasValue && itm.SourceEntityKey != store.Key && itm.TargetEntityKey.HasValue && itm.TargetEntityKey != store.Key) // The source was never meant for us
                        {
                            irelationships.RemoveRelationship(itm);
                        }
                        
                    }
                }

                // Remove MDM tags since this is a master
                if (store is ITaggable taggable)
                {
                    taggable.RemoveTag(MdmConstants.MdmTypeTag);
                    taggable.RemoveTag(MdmConstants.MdmGeneratedTag);
                    taggable.RemoveTag(MdmConstants.MdmRotIndicatorTag);
                    taggable.RemoveTag(MdmConstants.MdmResourceTag);
                }
                if (store is IVersionedEntity versioned)
                {
                    versioned.VersionSequence = null;
                    versioned.VersionKey = null;
                    versioned.PreviousVersionKey = null;
                }


                store.StripAssociatedItemSources();
            }
            else if (!store.Key.HasValue)
            {
                store.Key = Guid.NewGuid(); // Ensure that we have a key for the object.
            }
            else
            {
                var masterRel = this.m_dataManager.GetMasterRelationshipFor(e.Data.Key.Value);
            }

            // Is this a ROT?
            if (this.m_dataManager.IsRecordOfTruth(e.Data))
            {
                this.m_policyEnforcement.Demand(MdmPermissionPolicyIdentifiers.EstablishRecordOfTruth);
                store = this.m_dataManager.PromoteRecordOfTruth(store);
            }

            // Rewrite any relationships we need to
            if (sender is Bundle bundle)
            {
                if (originalKey != store.Key) // storage has changed
                {
                    bundle.Item.Insert(bundle.Item.FindIndex(o => o.Key == originalKey), store);
                    bundle.Item.RemoveAll(o => o.Key == originalKey); // Remove
                }
                bundle.Item.AddRange(this.m_dataManager.ExtractRelationships(store).OfType<IdentifiedData>());
                this.m_dataManager.RefactorRelationships(bundle.Item, originalKey.Value, store.Key.Value);

                // Rewrite the focal object to the proper objects actually being actioned
                if (originalKey != store.Key.Value)
                {
                    var replaceKeys = bundle.FocalObjects.Where(f => f == originalKey).ToArray();
                    if (replaceKeys.Any())
                    {
                        bundle.FocalObjects.Add(store.Key.Value);
                        bundle.FocalObjects.RemoveAll(f => f == originalKey);
                    }
                }
            }
            else
            {
                this.m_dataManager.RefactorRelationships(new List<IdentifiedData>() { store }, originalKey.Value, store.Key.Value);
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
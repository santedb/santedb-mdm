/*
 * Portions Copyright (C) 2019 - 2020, Fyfe Software Inc. and the SanteSuite Contributors (See NOTICE.md)
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
 * Date: 2020-2-2
 */
using SanteDB.Core;
using SanteDB.Core.Configuration;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Model.Acts;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Model.Serialization;
using SanteDB.Core.Services;
using SanteDB.Core.Services.Impl;
using SanteDB.Persistence.MDM.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Subscription;
using SanteDB.Core.Security;
using SanteDB.Core.Model.EntityLoader;
using SanteDB.Core.Data;
using SanteDB.Core.Model.Constants;
using SanteDB.Core.Model.Roles;
using SanteDB.Core.Model.Map;
using SanteDB.Core.Interfaces;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Exceptions;
using SanteDB.Persistence.MDM.Services.Resources;
using SanteDB.Core.Jobs;
using SanteDB.Persistence.MDM.Jobs;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Event;

namespace SanteDB.Persistence.MDM.Services
{
    /// <summary>
    /// The MdmRecordDaemon is responsible for subscribing to MDM targets in the configuration 
    /// and linking/creating master records whenever a record of that type is created.
    /// </summary>
    [ServiceProvider("MIDM Data Repository")]
    public class MdmDataManagementService : IDaemonService, IDisposable
    {

        /// <summary>
        /// Gets the service name
        /// </summary>
        public string ServiceName => "MIDM Daemon";

        // Matching service
        private IRecordMatchingService m_matchingService;
        // Service manager
        private IServiceManager m_serviceManager;
        // Sub executor
        private ISubscriptionExecutor m_subscriptionExecutor;
        // Job manager
        private IJobManagerService m_jobManager;
        // Entity relationship service
        private IDataPersistenceService<EntityRelationship> m_entityRelationshipService;
        // Entity service
        private IDataPersistenceService<Entity> m_entityService;
      
        // TRace source
        private Tracer m_traceSource = new Tracer(MdmConstants.TraceSourceName);

        // Configuration
        private ResourceMergeConfigurationSection m_configuration;

        // Listeners
        private List<IDisposable> m_listeners = new List<IDisposable>();

        // True if the service is running
        public bool IsRunning => this.m_listeners.Count > 0;

        /// <summary>
        /// Daemon is starting
        /// </summary>
        public event EventHandler Starting;
        /// <summary>
        /// Daemon is stopping
        /// </summary>
        public event EventHandler Stopping;
        /// <summary>
        /// Daemon has started
        /// </summary>
        public event EventHandler Started;
        /// <summary>
        /// Daemon has stopped
        /// </summary>
        public event EventHandler Stopped;

        /// <summary>
        /// Create injected service
        /// </summary>
        public MdmDataManagementService(IServiceManager serviceManager, IConfigurationManager configuration, IRecordMatchingService matchingService = null, ISubscriptionExecutor subscriptionExecutor = null, SimDataManagementService simDataManagementService = null, IJobManagerService jobManagerService = null)
        {
            this.m_configuration = configuration.GetSection<ResourceMergeConfigurationSection>();
            this.m_matchingService = matchingService;
            this.m_serviceManager = serviceManager;
            this.m_subscriptionExecutor = subscriptionExecutor;
            this.m_jobManager = jobManagerService;
            if (simDataManagementService != null)
            {
                throw new InvalidOperationException("Cannot run MDM and SIM in same mode");
            }
        }

        /// <summary>
        /// Dispose of this object
        /// </summary>
        public void Dispose()
        {
            foreach (var i in this.m_listeners)
                i.Dispose();
            this.m_listeners.Clear();
        }

        /// <summary>
        /// Start the daemon
        /// </summary>
        public bool Start()
        {
            this.Starting?.Invoke(this, EventArgs.Empty);

            // Pre-register types for serialization
            foreach (var itm in this.m_configuration.ResourceTypes)
            {
                if (itm.ResourceType == typeof(Entity))
                {
                    throw new InvalidOperationException("Cannot bind MDM control to Entity or Act , only sub-classes");
                }

                var rt = itm.ResourceType;
                string typeName = $"{rt.Name}Master";
                if (typeof(Entity).IsAssignableFrom(rt))
                    rt = typeof(EntityMaster<>).MakeGenericType(rt);
                else if (typeof(Act).IsAssignableFrom(rt))
                    rt = typeof(ActMaster<>).MakeGenericType(rt);
                ModelSerializationBinder.RegisterModelType(typeName, rt);

            }

            // Wait until application context is started
            ApplicationServiceContext.Current.Started += (o, e) =>
            {
                if (this.m_matchingService == null)
                    this.m_traceSource.TraceWarning("The MDM Service should be using a record matching service");

                // Replace matching
                var mdmMatcher = this.m_serviceManager.CreateInjected<MdmRecordMatchingService>();
                this.m_serviceManager.AddServiceProvider(mdmMatcher);

                if (this.m_matchingService != null)
                {
                    this.m_serviceManager.RemoveServiceProvider(this.m_matchingService.GetType());
                }

                foreach (var itm in this.m_configuration.ResourceTypes)
                {
                   
                    this.m_traceSource.TraceInfo("Adding MDM listener for {0}...", itm.ResourceType.Name);
                    MdmDataManagerFactory.RegisterDataManager(itm);
                    var idt = typeof(MdmResourceHandler<>).MakeGenericType(itm.ResourceType);
                    var ids = this.m_serviceManager.CreateInjected(idt) as IDisposable;
                    this.m_listeners.Add(ids);
                    this.m_serviceManager.AddServiceProvider(ids);
                    this.m_serviceManager.AddServiceProvider(MdmDataManagerFactory.CreateMerger(itm.ResourceType));

                    // Add job
                    var jobType = typeof(MdmMatchJob<>).MakeGenericType(itm.ResourceType);
                    var job = Activator.CreateInstance(jobType) as IJob;
                    this.m_jobManager?.AddJob(job, TimeSpan.MaxValue, JobStartType.Never);
                }

                // Add an entity relationship and act relationship watcher to the persistence layer for after update 
                // this will ensure that appropriate cleanup is performed on successful processing of data
                this.m_entityRelationshipService = ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityRelationship>>();
                this.m_entityService = ApplicationServiceContext.Current.GetService<IDataPersistenceService<Entity>>();

                ApplicationServiceContext.Current.GetService<IDataPersistenceService<Bundle>>().Inserted += RecheckBundleTrigger;
                ApplicationServiceContext.Current.GetService<IDataPersistenceService<Bundle>>().Updated += RecheckBundleTrigger;
                ApplicationServiceContext.Current.GetService<IDataPersistenceService<Bundle>>().Obsoleted += RecheckBundleTrigger;
                this.m_entityRelationshipService.Inserted += RecheckRelationshipTrigger;
                this.m_entityRelationshipService.Updated += RecheckRelationshipTrigger;
                this.m_entityRelationshipService.Obsoleted += RecheckRelationshipTrigger;

                // Add an MDM listener for subscriptions
                if (this.m_subscriptionExecutor != null)
                {
                    m_subscriptionExecutor.Executed += MdmSubscriptionExecuted;
                }
                this.m_listeners.Add(new BundleResourceInterceptor(this.m_listeners));

                // Slipstream the MdmEntityProvider
                EntitySource.Current = new EntitySource(new MdmEntityProvider());

                // FTS?
                if (ApplicationServiceContext.Current.GetService<IFreetextSearchService>() == null)
                    m_serviceManager.AddServiceProvider(new MdmFreetextSearchService());

            };

            this.Started?.Invoke(this, EventArgs.Empty);
            return true;
        }



        /// <summary>
        /// Re-check relationships to ensure that they are properly in the database
        /// </summary>
        private void RecheckRelationshipTrigger(object sender, DataPersistedEventArgs<EntityRelationship> e)
        {
            switch (e.Data.RelationshipTypeKey.ToString())
            {
                case MdmConstants.MASTER_RECORD_RELATIONSHIP:
                    // Is the data obsoleted (removed)? If so, then ensure we don't have a hanging master
                    if (e.Data.ObsoleteVersionSequenceId.HasValue)
                    {

                        if (this.m_entityRelationshipService.Any(r => r.TargetEntityKey == e.Data.TargetEntityKey && r.TargetEntity.StatusConceptKey != StatusKeys.Obsolete && r.SourceEntityKey != e.Data.SourceEntityKey && r.ObsoleteVersionSequenceId == null))
                        {
                            this.m_entityService.Obsolete(new Entity() { Key = e.Data.TargetEntityKey }, e.Mode, e.Principal);
                        }
                        return; // no need to de-dup check on obsoleted object
                    }

                    // MDM relationship should be the only active relationship between
                    // So when:
                    // A =[MDM-Master]=> B
                    // You cannot have:
                    // A =[MDM-Duplicate]=> B
                    // A =[MDM-Original]=> B
                    foreach (var itm in this.m_entityRelationshipService.Query(q => q.RelationshipTypeKey != MdmConstants.MasterRecordRelationship && q.SourceEntityKey == e.Data.SourceEntityKey && q.TargetEntityKey == e.Data.TargetEntityKey && q.ObsoleteVersionSequenceId == null, e.Principal))
                    {
                        itm.ObsoleteVersionSequenceId = Int32.MaxValue;
                        this.m_entityRelationshipService.Update(itm, e.Mode, e.Principal);
                    }
                    break;
                case MdmConstants.RECORD_OF_TRUTH_RELATIONSHIP:
                    // Is the ROT being assigned, and if so is there another ?
                    if (!e.Data.ObsoleteVersionSequenceId.HasValue)
                    {
                        foreach (var rotRel in this.m_entityRelationshipService.Query(r => r.SourceEntityKey == e.Data.SourceEntityKey && r.TargetEntityKey != e.Data.TargetEntityKey && r.ObsoleteVersionSequenceId == null, e.Principal))
                        {
                            //Obsolete other ROTs (there can only be one)
                            this.m_entityRelationshipService.Obsolete(rotRel.Key.Value, e.Mode, e.Principal);
                        }
                    }
                    break;

            }
        }

        /// <summary>
        /// Recheck the bundle trigger to ensure the relationships make sense
        /// </summary>
        private void RecheckBundleTrigger(object sender, DataPersistedEventArgs<Bundle> e)
        {
            foreach (var itm in e.Data.Item.OfType<EntityRelationship>())
            {
                this.RecheckRelationshipTrigger(sender, new DataPersistedEventArgs<EntityRelationship>(itm, e.Mode, e.Principal));
            }
        }

        /// <summary>
        /// Fired when the MDM Subscription has been executed
        /// </summary>
        private void MdmSubscriptionExecuted(object sender, Core.Event.QueryResultEventArgs<Core.Model.IdentifiedData> e)
        {

            var authPrincipal = AuthenticationContext.Current.Principal;

            // Results contain LOCAL records most likely
            // We have a resource type that matches
            e.Results = e.Results.AsParallel().AsOrdered().Select((res) =>
            {
                if (!this.m_configuration.ResourceTypes.Any(o => o.ResourceType == res.GetType())) return res;
                // Result is taggable and a tag exists for MDM
                if (res is Entity entity)
                {
                    if (entity.ClassConceptKey != MdmConstants.MasterRecordClassification)
                    {
                        // just a regular record
                        // Attempt to load the master and add to the results
                        var master = entity.LoadCollection<EntityRelationship>(nameof(Entity.Relationships)).FirstOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship);
                        if (master == null) // load from DB
                            master = EntitySource.Current.Provider.Query<EntityRelationship>(o => o.SourceEntityKey == entity.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship).FirstOrDefault();

                        var masterType = typeof(EntityMaster<>).MakeGenericType(res.GetType());

                        if (master != null)
                            return (Activator.CreateInstance(masterType, master.LoadProperty<Entity>(nameof(EntityRelationship.TargetEntity))) as IMdmMaster).GetMaster(authPrincipal);
                        else
                        {
                            entity.Tags.Add(new Core.Model.DataTypes.EntityTag("$mdm.type", "O")); // Orphan record
                            return entity;
                        }
                    }
                    else // is a master
                    {
                        var masterType = typeof(EntityMaster<>).MakeGenericType(MapUtil.GetModelTypeFromClassKey(entity.ClassConceptKey.Value));
                        return (Activator.CreateInstance(masterType, entity) as IMdmMaster).GetMaster(authPrincipal);
                    }
                }
                else if (res is Act act && act.ClassConceptKey != MdmConstants.MasterRecordClassification)
                {
                    // Attempt to load the master and add to the results
                    var master = act.LoadCollection<ActRelationship>(nameof(Act.Relationships)).FirstOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship);
                    if (master == null) // load from DB
                        master = EntitySource.Current.Provider.Query<ActRelationship>(o => o.SourceEntityKey == act.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship).FirstOrDefault();
                    var masterType = typeof(EntityMaster<>).MakeGenericType(res.GetType());
                    return (Activator.CreateInstance(masterType, master.LoadProperty<Act>(nameof(ActRelationship.TargetAct))) as IMdmMaster).GetMaster(authPrincipal);
                }
                else
                    throw new InvalidOperationException($"Result of type {res.GetType().Name} is not supported in MDM contexts");
            }).OfType<IdentifiedData>().ToArray();
        }

        /// <summary>
        /// Stop the daemon
        /// </summary>
        /// <returns></returns>
        public bool Stop()
        {
            this.Stopping?.Invoke(this, EventArgs.Empty);

            // Unregister
            foreach (var i in this.m_listeners)
                i.Dispose();
            this.m_listeners.Clear();

            this.Stopped?.Invoke(this, EventArgs.Empty);
            return true;
        }


    }
}

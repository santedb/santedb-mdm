/*
 * Copyright (C) 2021 - 2021, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2021-8-5
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
using SanteDB.Core.Matching;

namespace SanteDB.Persistence.MDM.Services
{
    /// <summary>
    /// The MdmRecordDaemon is responsible for subscribing to MDM targets in the configuration
    /// and linking/creating master records whenever a record of that type is created.
    /// </summary>
    [ServiceProvider("MIDM Data Repository")]
    public class MdmDataManagementService : IDaemonService, IDisposable, IDataManagementPattern
    {
        /// <summary>
        /// Gets the service name
        /// </summary>
        public string ServiceName => "MDM Data Management";

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

        // Match configuration service
        private IRecordMatchingConfigurationService m_matchConfigurationService;

        // TRace source
        private readonly Tracer m_traceSource = new Tracer(MdmConstants.TraceSourceName);

        // Configuration
        private ResourceManagementConfigurationSection m_configuration;

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
        public MdmDataManagementService(IServiceManager serviceManager, IConfigurationManager configuration, IRecordMatchingConfigurationService matchConfigurationService = null, IRecordMatchingService matchingService = null, ISubscriptionExecutor subscriptionExecutor = null, SimDataManagementService simDataManagementService = null, IJobManagerService jobManagerService = null)
        {
            this.m_configuration = configuration.GetSection<ResourceManagementConfigurationSection>();
            this.m_matchingService = matchingService;
            this.m_serviceManager = serviceManager;
            this.m_subscriptionExecutor = subscriptionExecutor;
            this.m_jobManager = jobManagerService;
            this.m_matchConfigurationService = matchConfigurationService;
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
                if (itm.Type == typeof(Entity))
                {
                    throw new InvalidOperationException("Cannot bind MDM control to Entity or Act , only sub-classes");
                }

                var rt = itm.Type;
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
                var mdmMatchConfig = this.m_serviceManager.CreateInjected<MdmMatchConfigurationService>();
                this.m_serviceManager.AddServiceProvider(mdmMatchConfig);
                if (this.m_matchingService != null)
                {
                    this.m_serviceManager.RemoveServiceProvider(this.m_matchingService.GetType());
                }
                if (this.m_matchConfigurationService != null)
                {
                    this.m_serviceManager.RemoveServiceProvider(this.m_matchConfigurationService.GetType());
                }

                foreach (var itm in this.m_configuration.ResourceTypes)
                {
                    this.m_traceSource.TraceInfo("Adding MDM listener for {0}...", itm.Type.Name);
                    MdmDataManagerFactory.RegisterDataManager(itm.Type);
                    var idt = typeof(MdmResourceHandler<>).MakeGenericType(itm.Type);
                    var ids = this.m_serviceManager.CreateInjected(idt) as IDisposable;
                    this.m_listeners.Add(ids);
                    this.m_serviceManager.AddServiceProvider(ids);
                    this.m_serviceManager.AddServiceProvider(MdmDataManagerFactory.CreateMerger(itm.Type));

                    // Add job
                    var jobType = typeof(MdmMatchJob<>).MakeGenericType(itm.Type);
                    var job = this.m_serviceManager.CreateInjected(jobType) as IJob;
                    this.m_jobManager?.AddJob(job, TimeSpan.MaxValue, JobStartType.Never);
                }

                // Add an entity relationship and act relationship watcher to the persistence layer for after update
                // this will ensure that appropriate cleanup is performed on successful processing of data
                this.m_entityRelationshipService = ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityRelationship>>();
                this.m_entityService = ApplicationServiceContext.Current.GetService<IDataPersistenceService<Entity>>();

                ApplicationServiceContext.Current.GetService<IDataPersistenceService<Bundle>>().Inserted += RecheckBundleTrigger;
                ApplicationServiceContext.Current.GetService<IDataPersistenceService<Bundle>>().Updated += RecheckBundleTrigger;
                ApplicationServiceContext.Current.GetService<IDataPersistenceService<Bundle>>().Deleted += RecheckBundleTrigger;
                this.m_entityRelationshipService.Inserted += RecheckRelationshipTrigger;
                this.m_entityRelationshipService.Updated += RecheckRelationshipTrigger;
                this.m_entityRelationshipService.Deleted += RecheckRelationshipTrigger;

                // Add an MDM listener for subscriptions
                if (this.m_subscriptionExecutor != null)
                {
                    m_subscriptionExecutor.Executed += MdmSubscriptionExecuted;
                }
                this.m_listeners.Add(new BundleResourceInterceptor(this.m_listeners));

                // Slipstream the MdmEntityProvider
                //EntitySource.Current = new EntitySource(new MdmEntityProvider());

                // HACK: Replace any freetext service with our own
                this.m_serviceManager.RemoveServiceProvider(typeof(IFreetextSearchService));
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
                    if (e.Data.ObsoleteVersionSequenceId.HasValue || e.Data.BatchOperation == Core.Model.DataTypes.BatchOperationType.Delete)
                    {
                        if (!this.m_entityRelationshipService.Query(r => r.TargetEntityKey == e.Data.TargetEntityKey && !StatusKeys.InactiveStates.Contains(r.TargetEntity.StatusConceptKey.Value) && r.SourceEntityKey != e.Data.SourceEntityKey && r.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal).Any())
                        {
                            this.m_entityService.Delete(e.Data.TargetEntityKey.Value, e.Mode, e.Principal, this.m_configuration.MasterDataDeletionMode);
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
                        this.m_entityRelationshipService.Delete(itm.Key.Value, e.Mode, e.Principal, this.m_configuration.MasterDataDeletionMode);
                    }

                    break;

                case MdmConstants.RECORD_OF_TRUTH_RELATIONSHIP:
                    // Is the ROT being assigned, and if so is there another ?
                    if (!e.Data.ObsoleteVersionSequenceId.HasValue || e.Data.BatchOperation == Core.Model.DataTypes.BatchOperationType.Delete)
                    {
                        foreach (var rotRel in this.m_entityRelationshipService.Query(r => r.SourceEntityKey == e.Data.SourceEntityKey && r.TargetEntityKey != e.Data.TargetEntityKey && r.ObsoleteVersionSequenceId == null, e.Principal))
                        {
                            //Obsolete other ROTs (there can only be one)
                            this.m_entityRelationshipService.Delete(rotRel.Key.Value, e.Mode, e.Principal, this.m_configuration.MasterDataDeletionMode);
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
            e.Results = new NestedQueryResultSet<IdentifiedData>(e.Results, (res) =>
            {
                if (!this.m_configuration.ResourceTypes.Any(o => o.Type == res.GetType())) return res;
                // Get the data manager for this type
                if (res is IHasClassConcept classifiable &&
                    classifiable.ClassConceptKey != MdmConstants.MasterRecordClassification)
                {
                    var dataManager = MdmDataManagerFactory.GetDataManager(res.GetType());
                    return dataManager.GetMasterFor(res.Key.Value).Synthesize(AuthenticationContext.Current.Principal) as IdentifiedData;
                }
                else
                {
                    return res;
                }
            });
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
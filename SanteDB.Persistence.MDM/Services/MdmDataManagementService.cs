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

        // TRace source
        private Tracer m_traceSource = new Tracer(MdmConstants.TraceSourceName);

        // Configuration
        private ResourceMergeConfigurationSection m_configuration = ApplicationServiceContext.Current.GetService<IConfigurationManager>().GetSection<ResourceMergeConfigurationSection>();

        // Listeners
        private List<MdmResourceListener> m_listeners = new List<MdmResourceListener>();

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
                if (ApplicationServiceContext.Current.GetService<IRecordMatchingService>() == null)
                    throw new InvalidOperationException("MDM requires a matching service to be configured");
                else if (ApplicationServiceContext.Current.GetService<SimDataManagementService>() != null)
                    throw new System.Configuration.ConfigurationException("Cannot use MDM and SIM merging strategies at the same time. Please disable one or the other");

                foreach (var itm in this.m_configuration.ResourceTypes)
                {
                    this.m_traceSource.TraceInfo("Adding MDM listener for {0}...", itm.ResourceType.Name);
                    var idt = typeof(MdmResourceListener<>).MakeGenericType(itm.ResourceType);
                    var ids = Activator.CreateInstance(idt, itm) as MdmResourceListener;
                    this.m_listeners.Add(ids);
                    ApplicationServiceContext.Current.GetService<IServiceManager>().AddServiceProvider(ids);
                }

                // Add an MDM listener for subscriptions
                var subscService = ApplicationServiceContext.Current.GetService<ISubscriptionExecutor>();
                if (subscService != null)
                {
                    subscService.Executing += MdmSubscriptionExecuting;
                    subscService.Executed += MdmSubscriptionExecuted;
                }
                this.m_listeners.Add(new BundleResourceListener(this.m_listeners));

                // FTS?
                if (ApplicationServiceContext.Current.GetService<IFreetextSearchService>() == null)
                    ApplicationServiceContext.Current.GetService<IServiceManager>().AddServiceProvider(new MdmFreetextSearchService());
            };

            this.Started?.Invoke(this, EventArgs.Empty);
            return true;
        }

        /// <summary>
        /// The subscription is executing
        /// </summary>
        private void MdmSubscriptionExecuting(object sender, Core.Event.QueryRequestEventArgs<IdentifiedData> e)
        {
            e.Count = e.Count + 1; // Fetch one additional record
            e.UseFuzzyTotals = true;
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

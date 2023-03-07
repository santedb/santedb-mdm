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
 * Date: 2022-5-30
 */
using SanteDB.Core;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Event;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Principal;

namespace SanteDB.Persistence.MDM.Services.Resources
{
    /// <summary>
    /// Represents a resource interceptor for bundle
    /// </summary>
    public class BundleResourceInterceptor : IDisposable
    {
        private readonly Tracer m_tracer = Tracer.GetTracer(typeof(BundleResourceInterceptor));

        // Listeners for chaining
        private IEnumerable<IDisposable> m_listeners;

        // Notify repository
        private INotifyRepositoryService<Bundle> m_notifyRepository;

        // Bundle Persistence
        private IDataPersistenceService<Bundle> m_bundlePersistence;

        /// <summary>
        /// Bundle resource listener
        /// </summary>
        public BundleResourceInterceptor(IEnumerable<IDisposable> listeners)
        {
            if (listeners == null)
            {
                throw new ArgumentNullException(nameof(listeners), "Listeners for chained invokation is required");
            }
            this.m_listeners = listeners;

            foreach (var itm in this.m_listeners)
            {
                this.m_tracer.TraceInfo("Bundles will be chained to {0}", itm.GetType().FullName);
            }

            this.m_notifyRepository = ApplicationServiceContext.Current.GetService<INotifyRepositoryService<Bundle>>();
            this.m_bundlePersistence = ApplicationServiceContext.Current.GetService<IDataPersistenceService<Bundle>>();
            // Subscribe
            this.m_notifyRepository.Inserting += this.OnPrePersistenceValidate;
            this.m_notifyRepository.Saving += this.OnPrePersistenceValidate;
            this.m_notifyRepository.Deleting += this.OnPrePersistenceValidate;
            this.m_notifyRepository.Inserting += this.OnInserting;
            this.m_notifyRepository.Saving += this.OnSaving;
            this.m_notifyRepository.Deleting += this.OnDeleting;
            this.m_notifyRepository.Retrieved += this.OnRetrieved;
            this.m_notifyRepository.Retrieving += this.OnRetrieving;
            this.m_notifyRepository.Querying += this.OnQuerying;
        }

        /// <summary>
        /// As a bundle, we call the base on the contents of the data
        /// </summary>
        protected void OnPrePersistenceValidate(object sender, DataPersistingEventArgs<Bundle> e)
        {
            e.Data = this.ChainInvoke(sender, e, e.Data, nameof(OnPrePersistenceValidate), typeof(DataPersistingEventArgs<>));
        }

        /// <summary>
        /// Chain invoke a bundle on inserted
        /// </summary>
        protected void OnInserting(object sender, DataPersistingEventArgs<Bundle> e)
        {
            e.Data = this.ChainInvoke(sender, e, e.Data, nameof(OnInserting), typeof(DataPersistingEventArgs<>));
        }

        /// <summary>
        /// Chain invoke a bundle
        /// </summary>
        protected void OnSaving(object sender, DataPersistingEventArgs<Bundle> e)
        {
            e.Data = this.ChainInvoke(sender, e, e.Data, nameof(OnSaving), typeof(DataPersistingEventArgs<>));
        }

        /// <summary>
        /// On obsoleting
        /// </summary>
        protected void OnDeleting(object sender, DataPersistingEventArgs<Bundle> e)
        {
            e.Data = this.ChainInvoke(sender, e, e.Data, nameof(OnDeleting), typeof(DataPersistingEventArgs<>));
        }

        /// <summary>
        /// Cannot query bundles
        /// </summary>
        protected void OnQuerying(object sender, QueryRequestEventArgs<Bundle> e)
        {
            throw new NotSupportedException("Cannot query bundles");
        }

        /// <summary>
        /// Cannot query bundles
        /// </summary>
        protected void OnRetrieved(object sender, DataRetrievedEventArgs<Bundle> e)
        {
            throw new NotSupportedException("Cannot retrieve bundles");
        }

        /// <summary>
        /// Cannot query bundles
        /// </summary>
        protected void OnRetrieving(object sender, DataRetrievingEventArgs<Bundle> e)
        {
            throw new NotSupportedException("Cannot retrieve bundles");
        }

        /// <summary>
        /// Performs a chain invokation on the bundle's contents
        /// </summary>
        private Bundle ChainInvoke(object sender, object eventArgs, Bundle bundle, string methodName, Type argType)
        {
            var principal = eventArgs.GetType().GetProperty("Principal")?.GetValue(eventArgs) as IPrincipal;
            if (principal == null)
            {
                throw new InvalidOperationException("Cannot determine the principal of this request. MDM requires an authenticated principal");
            }

            this.m_tracer.TraceInfo("Will chain-invoke {0} on {1} items", methodName, bundle.Item.Count);

            for (int i = 0; i < bundle.Item.Count; i++)
            {
                var data = bundle.Item[i];
                if (data == null)
                {
                    throw new InvalidOperationException($"Bundle object at index {i} is null");
                }
                else if (!(data is IHasClassConcept && data is IHasTypeConcept && data is IHasRelationships))
                {
                    continue;
                }

                var mdmHandler = typeof(MdmResourceHandler<>).MakeGenericType(data.GetType());
                var evtArgType = argType.MakeGenericType(data.GetType());
                var evtArgs = Activator.CreateInstance(evtArgType, data, TransactionMode.Commit, (eventArgs as SecureAccessEventArgs).Principal);

                foreach (IMdmResourceHandler hdlr in this.m_listeners?.Where(o => o?.GetType() == mdmHandler))
                {
                    switch (methodName)
                    {
                        case nameof(OnPrePersistenceValidate):
                            hdlr.OnPrePersistenceValidate(bundle, evtArgs);
                            break;

                        case nameof(OnSaving):
                            hdlr.OnSaving(bundle, evtArgs);
                            break;

                        case nameof(OnInserting):
                            hdlr.OnInserting(bundle, evtArgs);
                            break;

                        case nameof(OnDeleting):
                            hdlr.OnObsoleting(bundle, evtArgs);
                            break;

                        default:
                            throw new InvalidOperationException($"Cannot determine how to handle {methodName}");
                    }

                    // Cancel?
                    var subData = evtArgType.GetProperty("Data")?.GetValue(evtArgs) as IdentifiedData;
                    if (subData == null)
                    {
                        throw new InvalidOperationException($"Response to {hdlr.GetType().FullName}.{methodName} returned no data");
                    }
                    if (bundle.Item[i].Key == subData.Key)
                    {
                        bundle.Item[i] = subData;
                    }

                    if (eventArgs is DataPersistingEventArgs<Bundle> eclc)
                    {
                        eclc.Success |= eclc.Cancel |= (bool)evtArgType.GetProperty("Cancel")?.GetValue(evtArgs);
                    }
                }
            }

            // Now that bundle is processed , process it
            if ((eventArgs as DataPersistingEventArgs<Bundle>)?.Cancel == true && (methodName == "OnInserting" || methodName == "OnSaving"))
            {
                this.m_tracer.TraceInfo("Post-Running Triggers from Cancelled Bundle Handler on {0}", methodName);

                var businessRulesSerice = ApplicationServiceContext.Current.GetService<IBusinessRulesService<Bundle>>();
                bundle = businessRulesSerice?.BeforeInsert(bundle) ?? bundle;
                // Business rules shouldn't be used for relationships, we need to delay load the sources
                bundle.Item.OfType<EntityRelationship>().ToList().ForEach((i) =>
                {
                    if (i.SourceEntity == null)
                    {
                        var candidate = bundle.Item.Find(o => o.Key == i.SourceEntityKey) as Entity;
                        if (candidate != null)
                        {
                            i.SourceEntity = candidate;
                        }
                    }
                });
                bundle = this.m_bundlePersistence.Insert(bundle, TransactionMode.Commit, principal);
                bundle = businessRulesSerice?.AfterInsert(bundle) ?? bundle;
            }

            return bundle;
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
                this.m_notifyRepository.Deleting -= this.OnDeleting;
                this.m_notifyRepository.Querying -= this.OnQuerying;
            }
        }
    }
}
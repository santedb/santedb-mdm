using SanteDB.Core;
using SanteDB.Core.Configuration;
using SanteDB.Core.Event;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Principal;
using System.Text;

namespace SanteDB.Persistence.MDM.Services.Resources
{
    /// <summary>
    /// Represents a resource interceptor for bundle
    /// </summary>
    public class BundleResourceInterceptor : IDisposable
    {

        // Listeners for chaining
        private IEnumerable<IDisposable> m_listeners;

        // Notify repository
        private INotifyRepositoryService<Bundle> m_notifyRepository;

        /// <summary>
        /// Bundle resource listener
        /// </summary>
        public BundleResourceInterceptor(IEnumerable<IDisposable> listeners) 
        {
            this.m_listeners = listeners;

            this.m_notifyRepository = ApplicationServiceContext.Current.GetService<INotifyRepositoryService<Bundle>>();

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
        protected void OnObsoleting(object sender, DataPersistingEventArgs<Bundle> e)
        {
            e.Data = this.ChainInvoke(sender, e, e.Data, nameof(OnObsoleting), typeof(DataPersistingEventArgs<>));
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
            var principal = eventArgs.GetType().GetProperty("Principal").GetValue(eventArgs) as IPrincipal;
            for (int i = 0; i < bundle.Item.Count; i++)
            {
                var data = bundle.Item[i];
                var mdmHandler = typeof(MdmResourceHandler<>).MakeGenericType(data.GetType());
                var evtArgType = argType.MakeGenericType(data.GetType());
                var evtArgs = Activator.CreateInstance(evtArgType, data, (eventArgs as SecureAccessEventArgs).Principal);

                foreach (var hdlr in this.m_listeners.Where(o => o.GetType() == mdmHandler))
                {
                    var exeMethod = mdmHandler.GetMethod(methodName);
                    exeMethod.Invoke(hdlr, new Object[] { bundle, evtArgs });

                    // Cancel?
                    var subData = evtArgType.GetProperty("Data").GetValue(evtArgs) as IdentifiedData;
                    if (bundle.Item[i].Key == subData.Key)
                        bundle.Item[i] = subData;

                    if (eventArgs is DataPersistingEventArgs<Bundle>)
                        (eventArgs as DataPersistingEventArgs<Bundle>).Cancel |= (bool)evtArgType.GetProperty("Cancel").GetValue(evtArgs);

                }
            }

            // Now that bundle is processed , process it
            if ((eventArgs as DataPersistingEventArgs<Bundle>)?.Cancel == true && (methodName == "OnInserting" || methodName == "OnSaving"))
            {
                var businessRulesSerice = ApplicationServiceContext.Current.GetService<IBusinessRulesService<Bundle>>();
                bundle = businessRulesSerice?.BeforeInsert(bundle) ?? bundle;
                // Business rules shouldn't be used for relationships, we need to delay load the sources
                bundle.Item.OfType<EntityRelationship>().ToList().ForEach((i) =>
                {
                    if (i.SourceEntity == null)
                    {
                        var candidate = bundle.Item.Find(o => o.Key == i.SourceEntityKey) as Entity;
                        if (candidate != null)
                            i.SourceEntity = candidate;
                    }
                });
                bundle = ApplicationServiceContext.Current.GetService<IDataPersistenceService<Bundle>>().Insert(bundle, TransactionMode.Commit, principal);
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
                this.m_notifyRepository.Obsoleting -= this.OnObsoleting;
                this.m_notifyRepository.Querying -= this.OnQuerying;
            }
        }
    }
}

using SanteDB.Core;
using SanteDB.Core.Configuration;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Interop;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Acts;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Security;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Exceptions;
using SanteDB.Persistence.MDM.Services.Resources;
using SanteDB.Rest.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SanteDB.Persistence.MDM.Rest
{
    /// <summary>
    /// Exposees the $mdm-candidate API onto the REST layer
    /// </summary>
    public class MdmCandidateOperation : IApiChildResourceHandler
    {

        private Tracer m_tracer = Tracer.GetTracer(typeof(MdmCandidateOperation));

        // Configuration
        private ResourceMergeConfigurationSection m_configuration;

        // Batch service
        private IRepositoryService<Bundle> m_batchService;

        /// <summary>
        /// Candidate operations manager
        /// </summary>
        public MdmCandidateOperation(IConfigurationManager configurationManager)
        {
            this.m_configuration = configurationManager.GetSection<ResourceMergeConfigurationSection>();
            this.ParentTypes = this.m_configuration.ResourceTypes.Select(o => o.ResourceType).ToArray();
        }

        /// <summary>
        /// Gets the parent types
        /// </summary>
        public Type[] ParentTypes { get; }

        /// <summary>
        /// Gets the name of the resource
        /// </summary>
        public string Name => "mdm-candidate";

        /// <summary>
        /// Gets the type of properties which are returned
        /// </summary>
        public Type PropertyType => typeof(Bundle);

        /// <summary>
        /// Gets the capabilities of this
        /// </summary>
        public ResourceCapabilityType Capabilities => ResourceCapabilityType.Search | ResourceCapabilityType.Get | ResourceCapabilityType.Delete;

        /// <summary>
        /// Re-Runs the matching algorithm on the specified master
        /// </summary>
        public object Add(Type scopingType, object scopingKey, object item)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Gets the specified sub object
        /// </summary>
        public object Get(Type scopingType, object scopingKey, object key)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Query the candidate links
        /// </summary>
        public IEnumerable<object> Query(Type scopingType, object scopingKey, NameValueCollection filter, int offset, int count, out int totalCount)
        {
            var merger = ApplicationServiceContext.Current.GetService(typeof(IRecordMergingService<>).MakeGenericType(scopingType)) as IRecordMergingService;
            if (merger == null)
            {
                throw new InvalidOperationException("No merging service configuration");
            }

            var result = merger.GetMergeCandidates((Guid)scopingKey);
            totalCount = result.Count();
            return result.Skip(offset).Take(count);
        }

        /// <summary>
        /// Remove the specified key
        /// </summary>
        public object Remove(Type scopingType, object scopingKey, object key)
        {
            var merger = ApplicationServiceContext.Current.GetService(typeof(IRecordMergingService<>).MakeGenericType(scopingType)) as IRecordMergingService;
            if(merger == null)
            {
                throw new InvalidOperationException("No merging service configuration");
            }

            merger.Ignore((Guid)scopingKey, new Guid[] { (Guid)key });
            return null;
        }
    }
}

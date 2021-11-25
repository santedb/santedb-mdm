using SanteDB.Core;
using SanteDB.Core.Interop;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Model.Parameters;
using SanteDB.Core.Services;
using SanteDB.Rest.Common;
using System;
using System.Collections.Generic;
using System.Text;

namespace SanteDB.Persistence.MDM.Rest
{
    /// <summary>
    /// MDM Clear operation
    /// </summary>
    public class MdmClearOperation : MdmOperationBase
    {
        /// <summary>
        /// Clear operation
        /// </summary>
        public MdmClearOperation(IConfigurationManager configurationManager, IDataPersistenceService<Bundle> batchService) : base(configurationManager, batchService)
        {
        }

        /// <summary>
        /// Get the scope binding
        /// </summary>
        public override ChildObjectScopeBinding ScopeBinding => ChildObjectScopeBinding.Class | ChildObjectScopeBinding.Instance;

        /// <summary>
        /// Get the name of the operation
        /// </summary>
        public override string Name => "mdm-clear";

        /// <summary>
        /// Invoke the specified operation
        /// </summary>
        public override object Invoke(Type scopingType, object scopingKey, ParameterCollection parameters)
        {
            var merger = ApplicationServiceContext.Current.GetService(typeof(IRecordMergingService<>).MakeGenericType(scopingType)) as IRecordMergingService;
            if (merger == null)
            {
                throw new InvalidOperationException($"Cannot find merging service for {scopingType.Name}. Is it under MDM control?");
            }

            parameters.TryGet<bool>("globalReset", out bool globalReset);
            parameters.TryGet<bool>("linksOnly", out bool linksOnly);
            parameters.TryGet<bool>("includeVerified", out bool includeVerified);

            // Is this scoped call?
            if (scopingKey == null)
            {
                if (globalReset)
                {
                    merger.Reset(includeVerified, linksOnly);
                }
                else
                {
                    merger.ClearGlobalMergeCanadidates();
                    if (includeVerified)
                    {
                        merger.ClearGlobalIgnoreFlags();
                    }
                }
            }
            else if (scopingKey is Guid scopingObjectKey)
            {
                if (globalReset)
                {
                    merger.Reset(scopingObjectKey, includeVerified, linksOnly);
                }
                else
                {
                    merger.ClearMergeCandidates(scopingObjectKey);
                    if (includeVerified)
                    {
                        merger.ClearIgnoreFlags(scopingObjectKey);
                    }
                }
            }
            else
            {
                throw new InvalidOperationException("Cannot determine the operation");
            }

            // No result
            return null;
        }
    }
}
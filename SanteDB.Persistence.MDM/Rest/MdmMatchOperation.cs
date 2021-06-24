using SanteDB.Core.Model.Acts;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Model.Entities;
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
    /// Represents a re-linking operation
    /// </summary>
    /// <remarks>This operation forces a re-matching operation</remarks>
    public class MdmMatchOperation : MdmOperationBase
    {

        /// <summary>
        /// Creates a new configuration match operation
        /// </summary>
        public MdmMatchOperation(IConfigurationManager configurationManager, IRepositoryService<Bundle> batchService) : base(configurationManager, batchService)
        {

        }

        /// <summary>
        /// Gets the name of the operation
        /// </summary>
        public override string Name => "mdm-rematch";

        /// <summary>
        /// Perform  the operation
        /// </summary>
        public override object Invoke(Type scopingType, object scopingKey, ApiOperationParameterCollection parameters)
        {
            var dataManager = MdmDataManagerFactory.GetDataManager<Entity>(scopingType);
            if (dataManager == null)
            {
                throw new NotSupportedException($"MDM is not configured for {scopingType}");
            }

            if (!(scopingKey is Guid scopingObjectKey))
                throw new ArgumentOutOfRangeException($"Scoping object must be a UUID");

            // Load the current master from @scopingKey
            if (!dataManager.IsMaster(scopingObjectKey))
            {
                throw new KeyNotFoundException($"{scopingObjectKey} is not an MDM Master");
            }

            // Now - we want to prepare a transaction
            Bundle retVal = new Bundle();

            if (parameters.TryGet<bool>("clear", out bool clear) && clear)
            {
                foreach (var itm in dataManager.GetCandidateLocals(scopingObjectKey).Where(o => o.ClassificationKey == MdmConstants.AutomagicClassification))
                {
                    if (itm is EntityRelationship er)
                    {
                        er.ObsoleteVersionSequenceId = Int32.MaxValue;
                        retVal.Add(er);
                    }
                    else if (itm is ActRelationship ar)
                    {
                        ar.ObsoleteVersionSequenceId = Int32.MaxValue;
                        retVal.Add(ar);
                    }
                }
            }

            retVal.AddRange(dataManager.MdmTxMatchMasters(dataManager.MdmGet(scopingObjectKey).GetMaster(AuthenticationContext.Current.Principal) as Entity, retVal.Item));

            // Now we want to save?
            if (parameters.TryGet<bool>("commit", out bool commit) && commit)
            {
                try
                {
                    retVal = this.m_batchService.Insert(retVal);
                }
                catch (Exception e)
                {
                    this.m_tracer.TraceError("Error persisting re-match: {0}", e.Message);
                    throw new MdmException("Error persisting re-match operation", e);
                }
            }
            return retVal;
        }
    }
}

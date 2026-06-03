using DocumentFormat.OpenXml.Wordprocessing;
using SanteDB.Core.Data;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Acts;
using SanteDB.Core.Model.DataTypes;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Persistence.MDM.Services.Resources;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Principal;
using System.Text;

namespace SanteDB.Persistence.MDM.Services
{
    /// <summary>
    /// Generic link provider is a fallback for entities
    /// </summary>
    public class GenericLinkProvider : IDataManagedLinkProvider
    {

        /// <inheritdoc/>
        public event EventHandler<DataManagementLinkEventArgs> ManagedLinkEstablished;

        /// <inheritdoc/>
        public event EventHandler<DataManagementLinkEventArgs> ManagedLinkRemoved;

        /// <inheritdoc/>
        IEnumerable<ITargetedAssociation> IDataManagedLinkProvider.FilterManagedReferenceLinks(IEnumerable<ITargetedAssociation> forRelationships) => forRelationships.Where(o => o.AssociationTypeKey == MdmConstants.MasterRecordRelationship);

        /// <summary>
        /// Get the specific link provider for the provided source
        /// </summary>
        private IDataManagedLinkProvider GetSpecificLinkProvider(IdentifiedData forSource)
        {
            if(!(forSource is IHasClassConcept ihc))
            {
                return null;
            }

            if (ihc.ClassConceptKey == MdmConstants.MasterRecordClassification && forSource is IHasTypeConcept iht &&
                iht.TryResolveTypeConceptToType(out var mappedType) &&
                MdmDataManagerFactory.TryGetDataManager(mappedType, out var retVal))
            {
                return retVal;
            }

            return null;
        }

        /// <inheritdoc/>
        IdentifiedData IDataManagedLinkProvider.ResolveGoldenRecord(IdentifiedData forSource) => this.GetSpecificLinkProvider(forSource)?.ResolveGoldenRecord(forSource);

        /// <inheritdoc/>
        IdentifiedData IDataManagedLinkProvider.ResolveManagedRecord(IdentifiedData forSource) => this.GetSpecificLinkProvider(forSource)?.ResolveManagedRecord(forSource);

        /// <inheritdoc/>
        IdentifiedData IDataManagedLinkProvider.ResolveOwnedRecord(IdentifiedData forTarget, IPrincipal ownerPrincipal) => this.GetSpecificLinkProvider(forTarget)?.ResolveOwnedRecord(forTarget, ownerPrincipal);

    }
}

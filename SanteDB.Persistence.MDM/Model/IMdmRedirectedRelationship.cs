using SanteDB.Core.Model.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace SanteDB.Persistence.MDM.Model
{
    /// <summary>
    /// Represents a relationship that has been rewritten
    /// </summary>
    public interface IMdmRedirectedRelationship : ITargetedAssociation
    {
        /// <summary>
        /// Gets the original holder
        /// </summary>
        Guid? OriginalHolderKey { get; }

        /// <summary>
        /// Gets the original target
        /// </summary>
        Guid? OriginalTargetKey { get; }
    }
}
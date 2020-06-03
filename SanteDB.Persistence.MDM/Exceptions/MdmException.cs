using SanteDB.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SanteDB.Persistence.MDM.Exceptions
{
    /// <summary>
    /// Represents an underlying exception in the MDM layer
    /// </summary>
    public class MdmException : Exception
    {

        /// <summary>
        /// Gets the target record which caused the exception
        /// </summary>
        public IdentifiedData TargetRecord { get; }

        /// <summary>
        /// Creates a new MDM exception object
        /// </summary>
        public MdmException(IdentifiedData record, String message) : this (record, message, null)
        {
        }

        /// <summary>
        /// Creates a new mdm exception object
        /// </summary>
        public MdmException(IdentifiedData record, String message, Exception cause) : base(message, cause)
        {
            this.TargetRecord = record;
        }
    }
}

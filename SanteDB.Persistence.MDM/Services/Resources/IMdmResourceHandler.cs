using System;
using System.Collections.Generic;
using System.Text;

namespace SanteDB.Persistence.MDM.Services.Resources
{
    /// <summary>
    /// Mdm Resource Handler
    /// </summary>
    public interface IMdmResourceHandler
    {

        /// <summary>
        /// Generic version of on pre persistence validate
        /// </summary>
        void OnPrePersistenceValidate(object sender, object args);

        /// <summary>
        /// Generic version of on inserting
        /// </summary>
        void OnInserting(object sender, object args);

        /// <summary>
        /// Generic version of on saving
        /// </summary>
        void OnSaving(object sender, object args);

        /// <summary>
        /// Generic version of on-obsoleting
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="args"></param>
        void OnObsoleting(object sender, object args);

    }
}

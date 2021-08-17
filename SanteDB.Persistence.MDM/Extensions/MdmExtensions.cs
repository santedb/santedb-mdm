using SanteDB.Core.Model.Entities;
using SanteDB.Core.Security;
using SanteDB.Persistence.MDM.Model;
using SanteDB.Persistence.MDM.Services.Resources;
using System;
using System.Collections.Generic;
using System.Text;

namespace SanteDB.Persistence.MDM.Extensions
{
    /// <summary>
    /// MDM extension methods
    /// </summary>
    public static class MdmExtensions
    {

        /// <summary>
        /// Get master for <paramref name="me"/> or, if it is already a master or not MDM controlled return <paramref name="me"/>
        /// </summary>
        public static TEntity GetMaster<TEntity>(this Entity me) where TEntity : Entity, new()
        {

            var dataManager = MdmDataManagerFactory.GetDataManager<TEntity>();
            if(dataManager.IsMaster(me.Key.GetValueOrDefault()))
            {
                return new EntityMaster<TEntity>(me).Synthesize(AuthenticationContext.Current.Principal);
            }
            else if(me is TEntity te)
            {
                return te;
            }
            else
            {
                throw new InvalidOperationException("Cannot determine how to fetch master");
            }

        }

    }
}

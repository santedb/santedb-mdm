﻿/*
 * Copyright (C) 2021 - 2025, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2023-6-21
 */
using System;

namespace SanteDB.Persistence.MDM
{
    /// <summary>
    /// Represents a series of identifies related to the MDM policies for record management
    /// </summary>
    public static class MdmPermissionPolicyIdentifiers
    {

        /// <summary>
        /// The principal has unrestricted access to the MDM
        /// </summary>
        public const String UnrestrictedMdm = "1.3.6.1.4.1.33349.3.1.5.9.2.6";

        /// <summary>
        /// The principal has permission to create new master data records
        /// </summary>
        public const String WriteMdmMaster = UnrestrictedMdm + ".1";

        /// <summary>
        /// The principal has permission to read all locals from data records
        /// </summary>
        public const String ReadMdmLocals = UnrestrictedMdm + ".2";

        /// <summary>
        /// The principal has permission to merge MDM master records
        /// </summary>
        public const String MergeMdmMaster = UnrestrictedMdm + ".3";

        /// <summary>
        /// The principal has permission to merge MDM master records
        /// </summary>
        public const String EstablishRecordOfTruth = UnrestrictedMdm + ".4";


        /// <summary>
        /// The principal has permission to edit a MDM record of truth
        /// </summary>
        public const String EditRecordOfTruth = EstablishRecordOfTruth + ".1";
    }
}

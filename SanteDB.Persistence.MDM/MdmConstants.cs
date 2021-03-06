﻿/*
 * Portions Copyright (C) 2019 - 2020, Fyfe Software Inc. and the SanteSuite Contributors (See NOTICE.md)
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
 * Date: 2020-2-2
 */
using System;

namespace SanteDB.Persistence.MDM
{
    /// <summary>
    /// Represents an MDM constant
    /// </summary>
    public static class MdmConstants
    {

        public const string MASTER_RECORD_RELATIONSHIP = "97730a52-7e30-4dcd-94cd-fd532d111578";
        public const string CANDIDATE_RECORD_RELATIONSHIP = "56cfb115-8207-4f89-b52e-d20dbad8f8cc";
        public const string IGNORE_CANDIDATE_RELATIONSHIP = "decfb115-8207-4f89-b52e-d20dbad8f8cc";
        public const string RECORD_OF_TRUTH_RELATIONSHIP = "1C778948-2CB6-4696-BC04-4A6ECA140C20";
        /// <summary>
        /// Relationship used to represents a local/master relationship
        /// </summary>
        /// <remarks>Whenever the MDM persistence layer is used the system will link incoming records (dirty records)
        /// with a generated pristine record tagged as a master record.</remarks>
        public static readonly Guid MasterRecordRelationship = Guid.Parse(MASTER_RECORD_RELATIONSHIP);

        /// <summary>
        /// Relationship used to represent that a local record has a high probability of being a duplicate with a master record
        /// </summary>
        public static readonly Guid CandidateLocalRelationship = Guid.Parse(CANDIDATE_RECORD_RELATIONSHIP);

        /// <summary>
        /// Relationship to represent the ignoring of a duplicate
        /// </summary>
        public static readonly Guid IgnoreCandidateRelationship = Guid.Parse(IGNORE_CANDIDATE_RELATIONSHIP);

        /// <summary>
        /// Represents a record of truth, this is a record which is promoted on the master record such that it is the "true" version of the record
        /// </summary>
        public static readonly Guid MasterRecordOfTruthRelationship = Guid.Parse(RECORD_OF_TRUTH_RELATIONSHIP);

        /// <summary>
        /// Master record classification
        /// </summary>
        public static readonly Guid MasterRecordClassification = Guid.Parse("49328452-7e30-4dcd-94cd-fd532d111578");

        /// <summary>
        /// Determiner codes
        /// </summary>
        public static readonly Guid RecordOfTruthDeterminer = Guid.Parse("6B1D6764-12BE-42DC-A5DC-52FC275C4935");

        /// <summary>
        /// The name of the trace source to use for the MDM logs
        /// </summary>
        public const String TraceSourceName = "SanteDB.Persistence.MDM";

        /// <summary>
        /// MDM configuration name
        /// </summary>
        public const String ConfigurationSectionName = "santedb.mdm";

        /// <summary>
        /// Indicates that the source entity is a local, however it isn't a local in that the matcher "found" it, 
        /// rather it is a local which was the result of someone editing the master
        /// </summary>
        public static readonly Guid OriginalMasterRelationship = Guid.Parse("a2837281-7e30-4dcd-94cd-fd532d111578");

        /// <summary>
        /// Automatic linked data
        /// </summary>
        public static readonly Guid AutomagicClassification = Guid.Parse("4311E243-FCDF-43D0-9905-41FD231B1B51");

        /// <summary>
        /// Verified classification 
        /// </summary>
        public static readonly Guid VerifiedClassification = Guid.Parse("3B9365BA-C229-44C4-95AE-6489809A33F0");

        /// <summary>
        /// MDM Type tag
        /// </summary>
        public const string MdmTypeTag = "$mdm.type";

        /// <summary>
        /// MDM Processed Tag
        /// </summary>
        public const string MdmProcessedTag = "$mdm.processed";

        /// <summary>
        /// Identity match configuration
        /// </summary>
        public const string MdmIdentityMatchConfiguration = "$identity";
    }
}

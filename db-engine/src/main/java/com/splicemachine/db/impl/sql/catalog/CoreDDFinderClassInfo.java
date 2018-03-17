/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2018 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.io.FormatableInstanceGetter;

/**
 * FormatableInstanceGetter to load stored instances
 * of DependableFinder. Class is registered in RegisteredFormatIds
 * 
 * @see com.splicemachine.db.catalog.DependableFinder
 * @see com.splicemachine.db.iapi.services.io.RegisteredFormatIds
 *
 */
public class CoreDDFinderClassInfo extends FormatableInstanceGetter {

	public Object getNewInstance() 
	{
		switch (fmtId) 
		{
			/* DependableFinders */
			case StoredFormatIds.ALIAS_DESCRIPTOR_FINDER_V01_ID: 
			case StoredFormatIds.CONGLOMERATE_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.CONSTRAINT_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.DEFAULT_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.FILE_INFO_FINDER_V01_ID:
			case StoredFormatIds.SCHEMA_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.SPS_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.TABLE_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.TRIGGER_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.VIEW_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.TABLE_PERMISSION_FINDER_V01_ID:
			case StoredFormatIds.ROUTINE_PERMISSION_FINDER_V01_ID:
			case StoredFormatIds.COLUMNS_PERMISSION_FINDER_V01_ID:
		    case StoredFormatIds.SEQUENCE_DESCRIPTOR_FINDER_V01_ID:
		    case StoredFormatIds.PERM_DESCRIPTOR_FINDER_V01_ID:
		    case StoredFormatIds.ROLE_GRANT_FINDER_V01_ID:
				return new DDdependableFinder(fmtId);
			case StoredFormatIds.COLUMN_DESCRIPTOR_FINDER_V01_ID:
				return new DDColumnDependableFinder(fmtId);
			default:
				return null;
		}

	}
}

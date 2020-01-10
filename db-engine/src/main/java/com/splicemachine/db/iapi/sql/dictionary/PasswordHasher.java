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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.db.iapi.sql.dictionary;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.util.StringUtil;
import com.splicemachine.db.shared.common.reference.SQLState;

/**
 * <p>
 * This machine performs the hashing of Derby passwords.
 * </p>
 */
public  class   PasswordHasher
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

     /**
     * The encoding to use when converting the credentials to a byte array
     * that can be passed to the hash function in the configurable hash scheme.
     */
    private static final String ENCODING = "UTF-8";

    /**
     * Pattern that is prefixed to the stored password in the SHA-1
     * authentication scheme.
     */
    public static final String ID_PATTERN_SHA1_SCHEME = "3b60";

    /**
     * Pattern that is prefixed to the stored password in the configurable
     * hash authentication scheme.
     */
    public static final String ID_PATTERN_CONFIGURABLE_HASH_SCHEME = "3b61";

    /**
     * Pattern that is prefixed to the stored password in the configurable
     * hash authentication scheme if key stretching has been applied. This
     * scheme extends the configurable hash scheme by adding a random salt and
     * applying the hash function multiple times when generating the hashed
     * token.
     */
    public static final String
            ID_PATTERN_CONFIGURABLE_STRETCHED_SCHEME = "3b62";

    /**
     * Character that separates the hash value from the name of the hash
     * algorithm in the stored password generated by the configurable hash
     * authentication scheme.
     */
    private static final char SEPARATOR_CHAR = ':';

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private String  _messageDigestAlgorithm;
    private byte[]  _salt;  // can be null
    private int         _iterations;
    

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Construct from pieces. Used for databases at rev level 10.6 or later.
     * </p>
     */
    public  PasswordHasher
        (
         String messageDigestAlgorithm,
         byte[]   salt,
         int    iterations
         )
    {
        _messageDigestAlgorithm = messageDigestAlgorithm;
        _salt = salt;
        _iterations = iterations;
    }

    /**
     * <p>
     * Construct from a hashed BUILTIN password stored in the PropertyConglomerate
     * or from a SYSUSERS.HASHINGSCHEME column.
     * </p>
     */
    public  PasswordHasher( String hashingScheme )
    {
        if ( hashingScheme.startsWith( ID_PATTERN_CONFIGURABLE_HASH_SCHEME) )
        {
            _messageDigestAlgorithm = hashingScheme.substring
                ( hashingScheme.indexOf( SEPARATOR_CHAR) +  1);
            _salt = null;
            _iterations = 1;
        }
        else if ( hashingScheme.startsWith( ID_PATTERN_CONFIGURABLE_STRETCHED_SCHEME) )
        {
            int saltPos = hashingScheme.indexOf(SEPARATOR_CHAR) + 1;
            int iterPos = hashingScheme.indexOf(SEPARATOR_CHAR, saltPos) + 1;
            int algoPos = hashingScheme.indexOf(SEPARATOR_CHAR, iterPos) + 1;

            _salt = StringUtil.fromHexString
                ( hashingScheme, saltPos, iterPos - saltPos - 1 );
            _iterations = Integer.parseInt
                ( hashingScheme.substring(iterPos, algoPos - 1) );
            _messageDigestAlgorithm = hashingScheme.substring(algoPos);
        }
        else
        {
            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT
                    (
                        "Unknown authentication scheme for token " +
                        hashingScheme
                     );
            }
        }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Produce a hashed password using a plaintext username and password. Turn it into
     * a printable string.
     * </p>
     */
    public  String  hashPasswordIntoString( String userName, String password )
        throws StandardException
    {
        if (password == null) {
            return null;
        }

        byte[] userBytes;
        byte[] passwordBytes;
        try {
            userBytes = userName.getBytes(ENCODING);
            passwordBytes = password.getBytes(ENCODING);
        } catch (UnsupportedEncodingException uee) {
            // UTF-8 should always be available, so this should never happen.
            throw StandardException.plainWrapException(uee);
        }

        MessageDigest md = getEmptyMessageDigest();

        byte[] digest = null;
        for (int i = 0; i < _iterations; i++)
        {
            md.reset();
            if (digest != null) {
                md.update(digest);
            }
            md.update(userBytes);
            md.update(passwordBytes);
            if ( _salt != null) {
                md.update( _salt );
            }
            digest = md.digest();
        }

        return StringUtil.toHexString( digest, 0, digest.length );
    }
    private MessageDigest   getEmptyMessageDigest()
        throws StandardException
    {
        if ( _messageDigestAlgorithm == null ) { throw badMessageDigest( null ); }
        try {
            return MessageDigest.getInstance( _messageDigestAlgorithm );
        } catch (NoSuchAlgorithmException nsae) { throw badMessageDigest( nsae ); }
    }
    private StandardException   badMessageDigest( Throwable t )
    {
        String  digestName = (_messageDigestAlgorithm == null) ? "NULL" : _messageDigestAlgorithm;

        return StandardException.newException( SQLState.DIGEST_NO_SUCH_ALGORITHM, t, digestName );
    }

    /**
     * <p>
     * Encodes the hashing algorithm in a string suitable for storing in SYSUSERS.HASHINGSCHEME.
     * </p>
     */
    public  String  encodeHashingScheme( )
    {
        return hashAndEncode( "" );
    }
    
    /**
     * <p>
     * Hash a username/password pair and return an encoded representation suitable
     * for storing as a BUILTIN password value in the PropertyConglomerate.
     * </p>
     */
    public  String  hashAndEncode( String userName, String password )
        throws StandardException
    {
        String  stringDigest = hashPasswordIntoString( userName, password );

        return hashAndEncode( stringDigest );
    }
    private String  hashAndEncode( String stringDigest )
    {
        if (( _salt == null || _salt.length == 0) && _iterations == 1)
        {
            // No salt was used, and only a single iteration, which is
            // identical to the default hashing scheme in 10.6-10.8. Generate
            // a token on a format compatible with those old versions.
            return ID_PATTERN_CONFIGURABLE_HASH_SCHEME +
                stringDigest +
                SEPARATOR_CHAR + _messageDigestAlgorithm;
        } else {
            // Salt and/or multiple iterations was used, so we need to add
            // those parameters to the token in order to verify the credentials
            // later.
            return ID_PATTERN_CONFIGURABLE_STRETCHED_SCHEME +
                stringDigest +
                SEPARATOR_CHAR + StringUtil.toHexString( _salt, 0, _salt.length) +
                SEPARATOR_CHAR + _iterations + SEPARATOR_CHAR + _messageDigestAlgorithm;
        }
    }
}

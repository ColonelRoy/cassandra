/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.transport.messages;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.ISaslAwareAuthenticator.SaslAuthenticator;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ServerConnection;

import org.jboss.netty.buffer.ChannelBuffer;

/**
 * A SASL token message sent from client to server. Some SASL
 * mechanisms & clients may send an initial token before
 * receiving a challenge from the server.
 */
public class SaslResponse extends Message.Request
{
    public static final Message.Codec<SaslResponse> codec = new Message.Codec<SaslResponse>()
    {
        @Override
        public SaslResponse decode(ChannelBuffer body, int version)
        {
            if (version == 1)
                throw new ProtocolException("SASL Authentication is not supported in version 1 of the protocol");

            return new SaslResponse(CBUtil.readValue(body));
        }

        @Override
        public ChannelBuffer encode(SaslResponse response)
        {
            return CBUtil.valueToCB(response.token);
        }
    };

    private byte[] token;

    public SaslResponse(byte[] token)
    {
        super(Message.Type.AUTH_RESPONSE);
        this.token = token;
    }

    @Override
    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    @Override
    public Response execute(QueryState queryState)
    {
        try
        {
            SaslAuthenticator authenticator = ((ServerConnection) connection).getAuthenticator();
            byte[] challenge = authenticator.evaluateResponse(token == null ? new byte[0] : token);
            if (authenticator.isComplete())
            {
                AuthenticatedUser user = authenticator.getAuthenticatedUser();
                queryState.getClientState().login(user);
                // authentication is complete, send a ready message to the client
                return new AuthSuccess(challenge);
            }
            else
            {
                return new SaslChallenge(challenge);
            }
        }
        catch (AuthenticationException e)
        {
            return ErrorMessage.fromException(e);
        }
    }
}

//
// Connection.cpp
//
// Library: MongoDB
// Package: MongoDB
// Module:  Connection
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/SocketStream.h"
#include "Poco/Net/SecureStreamSocket.h"
#include "Poco/MongoDB/Connection.h"
#include "Poco/MongoDB/Database.h"
#include "Poco/URI.h"
#include "Poco/Format.h"
#include "Poco/NumberParser.h"
#include "Poco/Exception.h"
#include "Poco/MongoDB/ReplicaSet.h"
#include <common/logger_useful.h>
#include <boost/algorithm/string.hpp> 


namespace Poco {
namespace MongoDB {


Connection::SocketFactory::SocketFactory()
{
}


Connection::SocketFactory::~SocketFactory()
{
}


Poco::Net::StreamSocket Connection::SocketFactory::createSocket(const std::string& host, int port, Poco::Timespan connectTimeout, bool secure)
{
	
        Poco::Net::SocketAddress addr(host, static_cast<UInt16>(port));
        	
	Poco::Net::StreamSocket socket;

        if (secure){
           socket=static_cast<Poco::Net::SecureStreamSocket>(addr);
        }
        if (connectTimeout > 0)
	     socket.connect(addr, connectTimeout);
	else
             socket.connect(addr);
	return socket;
	
        /*throw Poco::NotImplementedException("Default SocketFactory implementation does not support SecureStreamSocket");*/
}


Connection::Connection():
	_address(),
	_socket()
{
}


Connection::Connection(const std::string& hostAndPort):
	_address(hostAndPort),
	_socket()
{
	connect();
}


Connection::Connection(const std::string& uri, SocketFactory& socketFactory):
	_address(),
	_socket()
{
	connect(uri, socketFactory);
}


Connection::Connection(const std::string& host, int port):
	_address(host, static_cast<UInt16>(port)),
	_socket()
{
	connect();
}


Connection::Connection(const Poco::Net::SocketAddress& addrs):
	_address(addrs),
	_socket()
{
	connect();
}


Connection::Connection(const Poco::Net::StreamSocket& socket):
	_address(socket.peerAddress()),
	_socket(socket)
{
}


Connection::~Connection()
{
	try
	{
		disconnect();
	}
	catch (...)
	{
	}
}


void Connection::connect()
{
	_socket.connect(_address);
}


void Connection::connect(const std::string& hostAndPort)
{
	_address = Poco::Net::SocketAddress(hostAndPort);
	connect();
}


void Connection::connect(const std::string& host, int port)
{
	_address = Poco::Net::SocketAddress(host, static_cast<UInt16>(port));
	connect();
}


void Connection::connect(const Poco::Net::SocketAddress& addrs)
{
	_address = addrs;
	connect();
}


void Connection::connect(const Poco::Net::StreamSocket& socket)
{
	_address = socket.peerAddress();
	_socket = socket;
}


void Connection::connect(const std::string& uri, SocketFactory& socketFactory)
{
        Poco::MongoDB::Database::LoggerWrapper log_wrapper;
        Poco::URI theURI(uri);
	if (theURI.getScheme() != "mongodb") throw Poco::UnknownURISchemeException(uri);

	std::string userInfo = theURI.getUserInfo();
	std::string temphost = theURI.getHost();
	Poco::UInt16 port = theURI.getPort();
	if (port == 0) port = 27017;

        std::vector<std::string> str_addresses;
        if (uri.find(',') != std::string::npos){
                char hook='N';
                std::string tempstr;
                std::string::const_iterator it  = uri.begin();
	        std::string::const_iterator end = uri.end();
                for ( ; it!=uri.end(); ++it){
                      char c = *it;
                      if(c != '@' && c != '/' && hook=='Y'){
                           tempstr+=c;
                      }
                      else if(c == '@'){
                           hook='Y';
                      }
                }
                Poco::UInt16 i=0;
                boost::split(str_addresses, tempstr, boost::is_any_of(","));
                for (std::vector<std::string>::iterator it = str_addresses.begin(); it != str_addresses.end(); ++it,i++){
                     std::vector<std::string> tempvector;
                     boost::split(tempvector, *it, boost::is_any_of(":"));
                     str_addresses[i]=tempvector[0];
                }

        }
        else{
            str_addresses.insert(str_addresses.begin(),temphost);
        }
        
	std::string databaseName = theURI.getPath();
	if (!databaseName.empty() && databaseName[0] == '/') databaseName.erase(0, 1);
	if (databaseName.empty()) databaseName = "admin";

	bool ssl = false;
	Poco::Timespan connectTimeout;
	Poco::Timespan socketTimeout;
	std::string authMechanism = Database::AUTH_SCRAM_SHA1;
        std::string authSource="admin";
        std::string readPreference="primary";

	Poco::URI::QueryParameters params = theURI.getQueryParameters();
	for (Poco::URI::QueryParameters::const_iterator it = params.begin(); it != params.end(); ++it)
	{
		if (it->first == "ssl")
		{
			ssl = (it->second == "true");
		}
		else if (it->first == "connectTimeoutMS")
		{
			connectTimeout = static_cast<Poco::Timespan::TimeDiff>(1000)*Poco::NumberParser::parse(it->second);
		}
		else if (it->first == "socketTimeoutMS")
		{
			socketTimeout = static_cast<Poco::Timespan::TimeDiff>(1000)*Poco::NumberParser::parse(it->second);
		}
		else if (it->first == "authMechanism")
		{
			authMechanism = it->second;
		}
                else if (it->first == "authSource")
		{
			authSource= it->second;
                        databaseName=authSource;
                        _authsource=authSource;
		}
                else if (it->first == "readPreference")
		{
			readPreference= it->second;
		}

	}

        
        Poco::UInt16 i=0;        
        for (std::vector<std::string>::iterator it = str_addresses.begin(); it != str_addresses.end(); ++it,i++)
	{

	     std::string host =*it;
             Poco::Net::StreamSocket socket=socketFactory.createSocket(host , port, connectTimeout, ssl);
	     connect(socket);

	     if (socketTimeout > 0)
	     {
		_socket.setSendTimeout(socketTimeout);
		_socket.setReceiveTimeout(socketTimeout);
	     }

	     if (!userInfo.empty())
	     {
		std::string username;
		std::string password;
		std::string::size_type pos = userInfo.find(':');
		if (pos != std::string::npos)
		{
			username.assign(userInfo, 0, pos++);
			password.assign(userInfo, pos, userInfo.size() - pos);
		}
		else username = userInfo;

		Database database(databaseName);
                bool re=database.authenticate(*this, username, password, authMechanism);
                /*if (!database.authenticate(*this, username, password, authMechanism))*/
                if(re){
                         if(str_addresses.size()>1){
                                
                                QueryRequest request("admin.$cmd");
		                request.setNumberToReturn(1);
		                request.selector().add("isMaster", 1);
                                ResponseMessage response;
                                sendRequest(request, response);
                                if (response.documents().size() > 0)
		                {
                                     Document::Ptr doc = response.documents()[0];
                                     if (doc->get<bool>("ismaster") && boost::iequals(readPreference, "primary"))
			             {
                                        break;
			             }
			             else if (boost::iequals(readPreference, "secondary"))
			             {
                                        break;                                        
			             }
		                }

                       }
                }
	        else {
                       Poco::NoPermissionException(Poco::format("Access to MongoDB database %s denied for user %s", databaseName, username));
                }
	   }
        }//for

}


void Connection::disconnect()
{
	_socket.close();
}


void Connection::sendRequest(RequestMessage& request)
{
	Poco::Net::SocketOutputStream sos(_socket);
	request.send(sos);
}


void Connection::sendRequest(RequestMessage& request, ResponseMessage& response)
{
	sendRequest(request);

	Poco::Net::SocketInputStream sis(_socket);
	response.read(sis);
}


} } // Poco::MongoDB

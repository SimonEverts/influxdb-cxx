// MIT License
//
// Copyright (c) 2020-2023 offa
// Copyright (c) 2019 Adam Wegrzynek
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

///
/// \author Adam Wegrzynek <adam.wegrzynek@cern.ch>
///

#include "HTTP.h"
#include "InfluxDBException.h"
#include <optional>

namespace influxdb::transports
{
    namespace
    {
        void checkResponse(const cpr::Response& resp)
        {
            if (resp.error)
            {
                // TODO: message may be empty here
                throw InfluxDBException{"Request error: (" + std::to_string(static_cast<int>(resp.error.code)) + ") " + resp.error.message};
            }
            if (!cpr::status::is_success(resp.status_code))
            {
                throw InfluxDBException{"Request failed: (" + std::to_string(resp.status_code) + ") " + resp.reason};
            }
        }

        std::string parseUrl(const std::string& url)
        {
            const auto questionMarkPosition = url.find('?');

            if (questionMarkPosition == std::string::npos)
            {
                return url;
            }
            if (url.at(questionMarkPosition - 1) == '/')
            {
                return url.substr(0, questionMarkPosition - 1);
            }
            return url.substr(0, questionMarkPosition);
        }

        std::string parseDatabaseName(const std::string& url)
        {
            auto dbParameterPosition = url.find("?db=");
            if (dbParameterPosition == std::string::npos) {
                dbParameterPosition = url.find("&db=");
            }

            if (dbParameterPosition == std::string::npos)
            {
                throw InfluxDBException{"No Database specified"};
            }

            auto dbName = url.substr(dbParameterPosition + 4);
            auto separatorPos = dbName.find("&");
            if (separatorPos != std::string::npos) {
                dbName = dbName.substr(0, separatorPos);
            }

            return dbName;
        }

        std::optional<std::string> parseRetentionPolicy(const std::string& url)
        {
            auto dbParameterPosition = url.find("?rp=");
            if (dbParameterPosition == std::string::npos) {
                dbParameterPosition = url.find("&rp=");
            }

            if (dbParameterPosition == std::string::npos) {
                return std::nullopt;
            }

            auto rpName = url.substr(dbParameterPosition + 4);
            auto separatorPos = rpName.find("&");
            if (separatorPos != std::string::npos) {
                rpName = rpName.substr(0, separatorPos);
            }

            return rpName;
        }
    }


    HTTP::HTTP(const std::string& url)
        : endpointUrl(parseUrl(url)), databaseName(parseDatabaseName(url)), retentionPolicyName(parseRetentionPolicy(url))
    {
        session.SetTimeout(cpr::Timeout{std::chrono::seconds{10}});
        session.SetConnectTimeout(cpr::ConnectTimeout{std::chrono::seconds{10}});
    }

    std::string HTTP::query(const std::string& query)
    {
        session.SetUrl(cpr::Url{endpointUrl + "/query"});

        auto parameters = cpr::Parameters{{"db", databaseName}, {"q", query}};
        if (retentionPolicyName.has_value()) {
            parameters.Add({"rp", retentionPolicyName.value()});
        }
        session.SetParameters(parameters);

        const auto response = session.Get();
        checkResponse(response);

        return response.text;
    }

    void HTTP::setBasicAuthentication(const std::string& user, const std::string& pass)
    {
        session.SetAuth(cpr::Authentication{user, pass, cpr::AuthMode::BASIC});
    }

    void HTTP::setApiToken(const std::string& apiToken)
    {
        header["Authorization"] = "Token " + apiToken;
    }

    void HTTP::send(std::string&& lineprotocol)
    {
        session.SetUrl(cpr::Url{endpointUrl + "/write"});

        cpr::Header sendHeader (cpr::Header{{"Content-Type", "application/json"}});
        sendHeader.insert(header.begin(), header.end());
        session.SetHeader(sendHeader);

        auto parameters = cpr::Parameters{{"db", databaseName}};
        if (retentionPolicyName.has_value()) {
            parameters.Add({"rp", retentionPolicyName.value()});
        }
        session.SetParameters(parameters);
        session.SetBody(cpr::Body{lineprotocol});

        const auto response = session.Post();

        checkResponse(response);
    }

    void HTTP::setProxy(const Proxy& proxy)
    {
        session.SetProxies(cpr::Proxies{{"http", proxy.getProxy()}, {"https", proxy.getProxy()}});

        if (const auto& auth = proxy.getAuthentication(); auth.has_value())
        {
            session.SetProxyAuth(cpr::ProxyAuthentication{{"http", cpr::EncodedAuthentication{auth->user, auth->password}},
                                                          {"https", cpr::EncodedAuthentication{auth->user, auth->password}}});
        }
    }

    std::string HTTP::execute(const std::string& cmd)
    {
        session.SetUrl(cpr::Url{endpointUrl + "/query"});

        auto parameters = cpr::Parameters{{"db", databaseName}, {"q", cmd}};
        if (retentionPolicyName.has_value()) {
            parameters.Add({"rp", retentionPolicyName.value()});
        }
        session.SetParameters(parameters);

        const auto response = session.Get();
        checkResponse(response);

        return response.text;
    }

    void HTTP::createDatabase()
    {
        session.SetUrl(cpr::Url{endpointUrl + "/query"});
        session.SetParameters(cpr::Parameters{{"q", "CREATE DATABASE " + databaseName}});

        const auto response = session.Post();
        checkResponse(response);
    }

} // namespace influxdb

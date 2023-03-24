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
#include "Transport.h"
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

        std::optional<std::string> parseParameter(const std::string& url, const std::string& name)
        {
            auto pos = url.find("?" + name + "=");
            if (pos == std::string::npos) {
                pos = url.find("&" + name + "=");
            }

            if (pos == std::string::npos) {
                return std::nullopt;
            }

            auto substr = url.substr(pos + 2 + name.length());
            auto separatorPos = substr.find("&");
            if (separatorPos != std::string::npos) {
                substr = substr.substr(0, separatorPos);
            }

            return substr;
        }

        std::optional<std::string> parseDatabaseName(const std::string& url)
        {
            return parseParameter(url, "db");
        }

        std::optional<std::string> parseRetentionPolicy(const std::string& url)
        {
            return parseParameter(url, "rp");
        }

        std::optional<std::string> parseBucket(const std::string& url)
        {
            return parseParameter(url, "bucket");
        }

        std::optional<std::string> parseOrganization(const std::string& url)
        {
            return parseParameter(url, "org");
        }
    }


    HTTP::HTTP(const std::string& url, EndpointVersion version)
        : endpointUrl(parseUrl(url)), databaseName(parseDatabaseName(url)),
        endpointVersion(version)
    {
        session.SetTimeout(cpr::Timeout{std::chrono::seconds{10}});
        session.SetConnectTimeout(cpr::ConnectTimeout{std::chrono::seconds{10}});

        databaseName = parseDatabaseName(url);
        retentionPolicyName = parseRetentionPolicy(url);
        bucketName = parseBucket(url);
        organization = parseOrganization(url);

        if (endpointVersion == EndpointVersion::v1) {
            if (!databaseName.has_value()) {
                throw InfluxDBException{"No Database specified in URL"};
            }

            if (bucketName.has_value()) {
                throw InfluxDBException{"Bucket provided in URL but not supported for endpoint v1"};
            }
        } else { // EndpointVersion::v2
            if (databaseName.has_value()) {
                throw InfluxDBException{"Database provided in URL but not supported for endpoint v2"};
            }
            if (retentionPolicyName.has_value()) {
                throw InfluxDBException{"Retention policy provided in URL but not supported for endpoint v2"};
            }

            if (!bucketName.has_value()) {
                throw InfluxDBException{"Bucket is required as URL parameter for endpoint version v2"};
            }
            if (!organization.has_value()) {
                throw InfluxDBException{"Organization is required as URL parameter for endpoint version v2"};
            }
        }
    }

    cpr::Parameters HTTP::parameters()
    {
        if (endpointVersion == EndpointVersion::v1) {
            auto parameters = cpr::Parameters{{"db", databaseName.value()}};
            if (retentionPolicyName.has_value()) {
                parameters.Add({"rp", retentionPolicyName.value()});
            }
            return parameters;
        }

        if (endpointVersion == EndpointVersion::v2) {
            auto parameters = cpr::Parameters{
                {"org", organization.value()},
                {"bucket", bucketName.value()}
                };
        
            return parameters;
        }

        throw InfluxDBException{"Not implemented for current endpoint version"};
    }

    std::string HTTP::query(const std::string& query)
    {
        session.SetUrl(cpr::Url{endpointUrl + "/query"});

        auto parameters = this->parameters();
        parameters.Add({"q", query});
        
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

        session.SetParameters(parameters());
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

        auto parameters = this->parameters();
        parameters.Add({"q", cmd});

        session.SetParameters(parameters);

        const auto response = session.Get();
        checkResponse(response);

        return response.text;
    }

    void HTTP::createDatabase()
    {
        if (endpointVersion != Transport::EndpointVersion::v1) {
            throw InfluxDBException("Database only supported for endpoint v1");
        }

        session.SetUrl(cpr::Url{endpointUrl + "/query"});
        session.SetParameters(cpr::Parameters{{"q", "CREATE DATABASE " + databaseName.value()}});

        const auto response = session.Post();
        checkResponse(response);
    }

} // namespace influxdb

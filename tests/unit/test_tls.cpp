// TLS handshake test — exercises the real OpenSSL handshake on the server side
// against an in-process OpenSSL client. The cert+key are generated at test
// startup so the test is hermetic and does not depend on filesystem state.

#include <gtest/gtest.h>
#include <netinet/in.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "connection.h"
#include "tls.h"

namespace {

// Generate a 2048-bit RSA self-signed cert valid for ~1 day and write it +
// the private key to `cert_path` and `key_path`. Returns true on success.
bool generate_self_signed(const std::string& cert_path, const std::string& key_path) {
    EVP_PKEY* pkey = EVP_RSA_gen(2048);
    if (!pkey) return false;

    X509* x509 = X509_new();
    if (!x509) {
        EVP_PKEY_free(pkey);
        return false;
    }
    ASN1_INTEGER_set(X509_get_serialNumber(x509), 1);
    X509_gmtime_adj(X509_getm_notBefore(x509), 0);
    X509_gmtime_adj(X509_getm_notAfter(x509), 60 * 60 * 24);  // 1 day
    X509_set_pubkey(x509, pkey);

    X509_NAME* name = X509_get_subject_name(x509);
    X509_NAME_add_entry_by_txt(name, "C", MBSTRING_ASC,
                               reinterpret_cast<const unsigned char*>("US"), -1, -1, 0);
    X509_NAME_add_entry_by_txt(name, "O", MBSTRING_ASC,
                               reinterpret_cast<const unsigned char*>("inference-router-test"), -1,
                               -1, 0);
    X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                               reinterpret_cast<const unsigned char*>("localhost"), -1, -1, 0);
    X509_set_issuer_name(x509, name);

    if (!X509_sign(x509, pkey, EVP_sha256())) {
        X509_free(x509);
        EVP_PKEY_free(pkey);
        return false;
    }

    FILE* cf = std::fopen(cert_path.c_str(), "wb");
    if (!cf) {
        X509_free(x509);
        EVP_PKEY_free(pkey);
        return false;
    }
    PEM_write_X509(cf, x509);
    std::fclose(cf);

    FILE* kf = std::fopen(key_path.c_str(), "wb");
    if (!kf) {
        X509_free(x509);
        EVP_PKEY_free(pkey);
        return false;
    }
    PEM_write_PrivateKey(kf, pkey, nullptr, nullptr, 0, nullptr, nullptr);
    std::fclose(kf);

    X509_free(x509);
    EVP_PKEY_free(pkey);
    return true;
}

struct TempCert {
    std::filesystem::path dir;
    std::string cert_path;
    std::string key_path;

    TempCert() {
        // Per-test temporary directory; mkdtemp() patches the trailing XXXXXX in place.
        auto base = std::filesystem::temp_directory_path();
        char buf[128];
        std::snprintf(buf, sizeof(buf), "%s/ir-tls-XXXXXX", base.c_str());
        char* p = ::mkdtemp(buf);
        if (p) {
            dir = p;
            cert_path = (dir / "cert.pem").string();
            key_path = (dir / "key.pem").string();
        }
    }
    ~TempCert() {
        if (!dir.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
        }
    }
};

}  // namespace

TEST(Tls, HandshakeAndRoundTripMessage) {
    ir::tls_global_init();

    TempCert tc;
    ASSERT_FALSE(tc.cert_path.empty());
    ASSERT_TRUE(generate_self_signed(tc.cert_path, tc.key_path)) << "cert generation failed";

    ir::TlsContext server;
    ir::TlsContext::Options sopts;
    sopts.cert_path = tc.cert_path;
    sopts.key_path = tc.key_path;
    ASSERT_TRUE(server.init_server(sopts));

    // Listen on a free port. The test thread runs SSL_accept(); the main thread
    // runs SSL_connect() via an OpenSSL client built ad-hoc.
    int lfd = ir::listen_tcp("127.0.0.1", 0, 4);
    ASSERT_GE(lfd, 0);
    struct sockaddr_in sa {};
    socklen_t slen = sizeof(sa);
    ASSERT_EQ(::getsockname(lfd, reinterpret_cast<struct sockaddr*>(&sa), &slen), 0);
    std::uint16_t port = ntohs(sa.sin_port);

    // Server thread: accept, handshake, echo one message.
    std::atomic<bool> server_ok{false};
    std::thread srv([&] {
        int cfd = ::accept(lfd, nullptr, nullptr);
        if (cfd < 0) return;
        SSL* ssl = server.accept_handshake(cfd, 5000);
        if (!ssl) return;
        ir::Stream s = ir::Stream::tls(ssl);
        std::vector<std::uint8_t> req;
        if (ir::read_message(s, req, 64 * 1024, 5000) != ir::IoStatus::kOk) {
            ir::close_stream(s);
            return;
        }
        if (ir::write_message(s, req, 5000) != ir::IoStatus::kOk) {
            ir::close_stream(s);
            return;
        }
        ir::close_stream(s);
        server_ok.store(true);
    });

    // Client side: OpenSSL TLS client, sends a message and reads echo.
    int cfd = ir::dial_tcp("127.0.0.1", port, 5000);
    ASSERT_GE(cfd, 0);
    SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
    ASSERT_NE(ctx, nullptr);
    SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);  // self-signed; skip verify in this test
    SSL* css = SSL_new(ctx);
    ASSERT_NE(css, nullptr);
    SSL_set_fd(css, cfd);
    int hc = SSL_connect(css);
    ASSERT_GT(hc, 0) << "SSL_connect failed";

    ir::Stream client_stream = ir::Stream::tls(css);
    std::vector<std::uint8_t> payload = {'h', 'i', 't', 'l', 's'};
    ASSERT_EQ(ir::write_message(client_stream, payload, 5000), ir::IoStatus::kOk);
    std::vector<std::uint8_t> got;
    ASSERT_EQ(ir::read_message(client_stream, got, 64 * 1024, 5000), ir::IoStatus::kOk);
    EXPECT_EQ(got, payload);
    ir::close_stream(client_stream);
    SSL_CTX_free(ctx);

    srv.join();
    EXPECT_TRUE(server_ok.load());
    ir::safe_close(lfd);
}

TEST(Tls, InitServerFailsOnMissingCert) {
    ir::tls_global_init();
    ir::TlsContext ctx;
    ir::TlsContext::Options o;
    o.cert_path = "/tmp/does-not-exist-cert-9c5d2a.pem";
    o.key_path = "/tmp/does-not-exist-key-9c5d2a.pem";
    EXPECT_FALSE(ctx.init_server(o));
}

TEST(Tls, InitServerFailsOnEmptyPaths) {
    ir::tls_global_init();
    ir::TlsContext ctx;
    ir::TlsContext::Options o;
    EXPECT_FALSE(ctx.init_server(o));
}

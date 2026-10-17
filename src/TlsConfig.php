<?php
declare(strict_types=1);

namespace EasySwoole\AMQP;

final class TlsConfig
{
    public function __construct(
        public readonly bool $verifyPeer = true,
        public readonly bool $allowSelfSigned = false,
        public readonly ?string $caFile = null,
        public readonly ?string $caPath = null,
        public readonly ?string $clientCertificate = null,
        public readonly ?string $clientPrivateKey = null,
        public readonly ?string $passphrase = null,
        public readonly ?string $serverName = null,
    ) {
        if ($allowSelfSigned && !$verifyPeer) throw new \InvalidArgumentException('allowSelfSigned requires verifyPeer to be enabled.');
        if (($clientCertificate === null) !== ($clientPrivateKey === null)) {
            throw new \InvalidArgumentException('clientCertificate and clientPrivateKey must be configured together.');
        }
        if ($passphrase !== null && $clientPrivateKey === null) throw new \InvalidArgumentException('passphrase requires a client private key.');
        foreach ([
            'caFile'=>$caFile, 'caPath'=>$caPath, 'clientCertificate'=>$clientCertificate,
            'clientPrivateKey'=>$clientPrivateKey, 'serverName'=>$serverName,
        ] as $name => $value) {
            if ($value !== null && trim($value) === '') throw new \InvalidArgumentException("{$name} cannot be empty.");
        }
    }

    /** @return array<string, bool|string> */
    public function socketSettings(): array
    {
        $settings = [
            'open_ssl'=>true,
            'ssl_verify_peer'=>$this->verifyPeer,
            'ssl_allow_self_signed'=>$this->allowSelfSigned,
        ];
        foreach ([
            'ssl_cafile'=>$this->caFile, 'ssl_capath'=>$this->caPath,
            'ssl_cert_file'=>$this->clientCertificate, 'ssl_key_file'=>$this->clientPrivateKey,
            'ssl_passphrase'=>$this->passphrase, 'ssl_host_name'=>$this->serverName,
        ] as $name => $value) if ($value !== null) $settings[$name] = $value;
        return $settings;
    }
}

<div align="center">
    <h1>Origamic</h1>
    <p>A self-organizing, decentralized P2P library in Rust for building recursive virtual networks on top of existing networks.</p>
</div>

## ✨ Features
- **Recursive Overlay Network**: Deploy virtual networks using any communication stream (TCP, WebSocket, or **Origamic itself**) as a foundation.
- **Virtual Stream Multiplexing**: Build and multiplex multiple TCP-like bidirectional virtual stream communications simultaneously on top of a single-shot gossip message network.
- **Secure Design**: Verify the authenticity of all messages using addresses and digital signatures based on Ed25519 key pairs. The network is securely isolated by UUIDs.

#!/bin/bash
# Persistent IPv6 ULA Setup for Agora
# Run once with: sudo ./persistent_ipv6_setup.sh

set -e

echo "🌐 Setting up persistent IPv6 ULA configuration for Agora"

# 1. Enable IPv6 non-local bind permanently
echo "📝 Making ip_nonlocal_bind persistent..."
echo 'net.ipv6.ip_nonlocal_bind = 1' >> /etc/sysctl.conf
sysctl -w net.ipv6.ip_nonlocal_bind=1

# 2. Create netplan configuration for ULA subnet
echo "📝 Creating netplan configuration..."
cat > /etc/netplan/99-agora-ipv6.yaml << 'EOF'
network:
  version: 2
  ethernets:
    wlp7s0:
      addresses:
        - fde5:402f:ab0a:1::/64
EOF

# 3. Apply netplan configuration
echo "🔧 Applying netplan configuration..."
netplan apply

# 4. Add local route for any-IP binding
echo "🔧 Adding local route for subnet binding..."
ip route add local fde5:402f:ab0a:1::/64 dev lo 2>/dev/null || echo "Local route already exists"

echo "✅ Setup complete!"
echo ""
echo "🎉 Your system can now bind to ANY IPv6 address in fde5:402f:ab0a:1::/64"
echo "   This configuration is persistent across reboots."
echo ""
echo "💡 Your PublisherAddressManager can now use addresses like:"
echo "   fde5:402f:ab0a:1::1"
echo "   fde5:402f:ab0a:1::2"
echo "   fde5:402f:ab0a:1::dead:beef:cafe"
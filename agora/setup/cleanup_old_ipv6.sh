#!/bin/bash
# Clean up old IPv6 configuration before applying new one

echo "🧹 Cleaning up old IPv6 configuration..."

# Remove old subnet from interface
sudo ip -6 addr del fd12:3456:7890:1::/64 dev wlp7s0 2>/dev/null || echo "Old subnet not found (OK)"

# Remove old local route
sudo ip route del local fd12:3456:7890:1::/64 dev lo 2>/dev/null || echo "Old local route not found (OK)"

# Clean up individual address routes from Python script
echo "🧹 Removing individual address routes..."
sudo ip -6 route show | grep "fd12:3456:7890:1:" | while read route; do
    sudo ip -6 route del $route 2>/dev/null || true
done

echo "✅ Cleanup complete!"
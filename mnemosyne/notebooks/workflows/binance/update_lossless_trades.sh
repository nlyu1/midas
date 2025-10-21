# Run in `midas` directory: sh ./mnemosyne/notebooks/workflows/binance/update_lossless_trades.sh
./target/release/binance-spot-trades --recompute-universe --peg-symbol USDC
./target/release/binance-futures-trades --recompute-universe --peg-symbol USDC

./target/release/binance-spot-trades --recompute-universe --peg-symbol USDT
./target/release/binance-futures-trades --recompute-universe --peg-symbol USDT
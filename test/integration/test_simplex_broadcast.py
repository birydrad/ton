import asyncio
import logging
import os
import shutil
from pathlib import Path

from tontester.install import Install
from tontester.network import FullNode, Network
from tontester.zerostate import SimplexConsensusConfig


def read_logs(nodes: list[FullNode]) -> str:
    parts: list[str] = []
    for node in nodes:
        if node.log_path.exists():
            parts.append(node.log_path.read_text(errors="ignore"))
    return "\n".join(parts)


async def main():
    repo_root = Path(__file__).resolve().parents[2]
    working_dir = repo_root / "test/integration/.simplex-broadcast"
    shutil.rmtree(working_dir, ignore_errors=True)
    working_dir.mkdir(exist_ok=True)

    algorithm = os.environ.get("TON_TEST_BROADCAST_ALGORITHM", "plumtree")
    validator_args = [
        "--experimental-public-broadcast",
        algorithm,
        "--experimental-fast-sync-broadcast",
        algorithm,
        "--experimental-private-broadcast",
        algorithm,
    ]

    install = Install(repo_root / "build", repo_root)
    install.tonlibjson.client_set_verbosity_level(1)

    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s][%(asctime)s][%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H-%M-%S",
    )

    simplex = SimplexConsensusConfig(
        target_block_rate_ms=1000,
        slots_per_leader_window=4,
        first_block_timeout_ms=1000,
        max_leader_window_desync=2,
    )

    async with Network(install, working_dir) as network:
        network.config.mc_consensus = simplex
        network.config.shard_consensus = simplex

        dht = network.create_dht_node()

        nodes: list[FullNode] = []
        for _ in range(4):
            node = network.create_full_node(validator_args=validator_args, separate_validator_addr=True)
            node.make_initial_validator()
            node.announce_to(dht)
            nodes.append(node)

        async with asyncio.TaskGroup() as start_group:
            _ = start_group.create_task(dht.run())
            for node in nodes:
                _ = start_group.create_task(node.run())

        await network.wait_mc_block(seqno=100)

        logs = read_logs(nodes)
        for category in ("public", "fast-sync", "private"):
            assert f"experimental broadcast algorithm category={category}" in logs
        for source in ("called_from=public", "called_from=fast-sync", "called_from=validator_session"):
            assert source in logs
        assert "no QuicServer for local id" not in logs
        assert "stream size limit exceeded" not in logs


if __name__ == "__main__":
    asyncio.run(asyncio.wait_for(main(), 10 * 60))

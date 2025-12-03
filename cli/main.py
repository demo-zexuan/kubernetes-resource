from __future__ import annotations
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

import typer
from loguru import logger
from tqdm import tqdm
from importlib.resources import files as ir_files

app = typer.Typer(help="统一的 Kubernetes 部署 CLI")


class K8sStack(object):
    def __init__(self, stack_code: str, stack_description: str, files: List[str]):
        self.stack_code = stack_code
        self.stack_description = stack_description
        self.files: List[str] = files


SUPPORT_STACKS = {
    "monitoring": K8sStack("monitoring", "Prometheus 与 Grafana 监控栈", [
        "namespace.yaml",
        "prometheus-rbac.yaml",
        "prometheus-config.yaml",
        "prometheus-deployment.yaml",
        "prometheus-service.yaml",
        "grafana-deployment.yaml",
        "grafana-service.yaml",
        "monitoring-ingress.yaml",
    ]),

    "elk": K8sStack("elk", "ELK 日志收集栈", [
        # 待添加
    ]),

    "hadoop": K8sStack("hadoop", "Hadoop 大数据处理栈", [
        # 待添加
    ]),
}


def complete_stack(incomplete: str) -> List[str]:
    # suggest stack names that start with the typed fragment
    stacks = list(SUPPORT_STACKS.keys())
    return [s for s in stacks if s.startswith(incomplete.lower())]


def run_cmd(cmd: List[str], cwd: Optional[Path] = None, check: bool = True) -> subprocess.CompletedProcess:
    """
    运行子进程命令。
    :param cmd:  命令列表
    :param cwd:  工作目录
    :param check: 是否检查返回码
    :return:  子进程完成结果
    """
    logger.debug(f"执行命令: {' '.join(cmd)} (cwd={cwd})")
    return subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=check)


def ensure_kubectl() -> None:
    """
    确保 kubectl 可用且已配置上下文。
    失败时抛出异常。
    """
    try:
        run_cmd(["kubectl", "version", "--client"], check=True)
    except Exception as e:
        logger.error("未检测到 kubectl，请安装并确保其在 PATH 中。")
        raise e
    try:
        run_cmd(["kubectl", "config", "current-context"], check=True)
    except Exception as e:
        logger.error("未配置当前 Kubernetes 上下文，请先配置 kubectl。")
        raise e


def apply_yaml(file_path: Path, dry_run: bool = False) -> None:
    """
    应用指定的 YAML 清单文件。
    :param file_path: YAML 文件路径
    :param dry_run:  是否进行服务端干跑
    """
    if not file_path.exists():
        logger.error(f"未找到清单文件: {file_path}")
        raise FileNotFoundError(file_path)
    cmd = ["kubectl", "apply", "-f", str(file_path)]
    if dry_run:
        cmd += ["--dry-run=server"]
    run_cmd(cmd)


def kubectl_wait(label: str, namespace: str, timeout: str = "300s") -> None:
    """
    等待指定标签的 Pod 就绪。
    :param label: Pod 标签
    :param namespace:  命名空间
    :param timeout: 等待超时时长
    """
    cmd = ["kubectl", "wait", "--for=condition=ready", "pod", "-l", label, "-n", namespace, f"--timeout={timeout}"]
    run_cmd(cmd)


def resource_dir_for(stack: str) -> Path:
    # Resolves to packaged resources under cli/resource/<stack>
    base = ir_files("cli").joinpath("resource").joinpath(stack)
    return Path(str(base))


@app.command("deploy")
def deploy(
        stack: str = typer.Argument(..., help="栈名称: 例如monitor", autocompletion=complete_stack),
        dry_run: bool = typer.Option(False, "--dry-run", help="服务端干跑"),
        timeout: str = typer.Option("300s", "--timeout", help="等待超时时长, 如 300s"),
        verbose: bool = typer.Option(False, "--verbose", "-v", help="详细日志"),
):
    """
    部署指定栈到 Kubernetes。
    """
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if verbose else "INFO", backtrace=False, diagnose=False)

    ensure_kubectl()

    if stack == "monitoring":
        monitor_dir = resource_dir_for("monitoring")
        logger.info("开始部署 Prometheus 与 Grafana 监控栈...")

        files = [monitor_dir / name for name in SUPPORT_STACKS["monitoring"].files]

        with tqdm(total=len(files), desc="应用清单", unit="个", ncols=100) as pbar:
            for fp in files:
                try:
                    tqdm.write(f"应用: {fp.name}")
                    apply_yaml(fp, dry_run=dry_run)
                    pbar.update(1)
                except Exception as e:
                    tqdm.write(f"失败: {fp.name} -> {e}")
                    logger.exception(f"应用清单失败: {fp}")
                    raise typer.Exit(code=1)

        if not dry_run:
            with tqdm(total=2, desc="等待Pod就绪", unit="组", ncols=100) as pbar:
                tqdm.write("等待 Prometheus Pod 就绪...")
                kubectl_wait(label="app=prometheus", namespace='monitoring', timeout=timeout)
                pbar.update(1)

                tqdm.write("等待 Grafana Pod 就绪...")
                kubectl_wait(label="app=grafana", namespace='monitoring', timeout=timeout)
                pbar.update(1)

            logger.success("部署完成！")
            logger.info("通过 Ingress 访问：")
            logger.info("Prometheus: http://k8s.local/prometheus")
            logger.info("Grafana:    http://k8s.local/grafana")
        else:
            logger.info("干跑完成，未实际创建资源。")
    else:
        logger.error(f"未知栈 '{stack}'，可用: monitoring")
        raise typer.Exit(code=2)


@app.command("list")
def list_stacks():
    """
    列出支持的 Kubernetes 栈。
    """
    print("支持的 Kubernetes 栈:")
    for stack_code, stack in SUPPORT_STACKS.items():
        print(f"- {stack_code}: {stack.stack_description}")


@app.command("delete")
def delete_stack(
        stack: str = typer.Argument(..., help="栈名称: 例如monitoring", autocompletion=complete_stack),
        verbose: bool = typer.Option(False, "--verbose", "-v", help="详细日志"),
):
    """
    删除指定栈的 Kubernetes 资源。
    """
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if verbose else "INFO", backtrace=False, diagnose=False)

    ensure_kubectl()

    cmd = ["kubectl", "get", "namespaces", stack]
    result = run_cmd(cmd, check=False)
    if result.returncode != 0:
        logger.warning(f"命名空间 '{stack}' 不存在，跳过删除。")
        raise typer.Exit(code=0)

    if stack == "monitoring":
        logger.info("开始删除 Prometheus 与 Grafana 监控栈...")
        cmd = ["kubectl", "delete", "namespaces", "monitoring"]
        run_cmd(cmd)
    else:
        logger.error(f"未知栈 '{stack}'，可用: monitoring")
        raise typer.Exit(code=2)


if __name__ == "__main__":
    app()

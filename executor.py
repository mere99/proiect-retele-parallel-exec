"""
Executie paralela reala + incarcare dinamica a claselor Python.
"""

import importlib.util
import os
import sys
import threading
import time
from pathlib import Path

# Director unde salvam clasele primite de la alti noduri.
CLASSES_DIR = Path(__file__).resolve().parent / "loaded_classes"


class Executor:
    """
    - incarca o clasa din fisier (local sau descarcat)
    - ruleaza o metoda pe N fire reale (threading.Thread)
    """

    def __init__(self):
        CLASSES_DIR.mkdir(parents=True, exist_ok=True)
        self._registry_lock = threading.Lock()
        self._loaded = {}  # class_name -> module object

    def has_class(self, class_name: str) -> bool:
        with self._registry_lock:
            if class_name in self._loaded:
                return True
        local = Path(__file__).resolve().parent / "tasks" / f"{class_name}.py"
        if local.is_file():
            return True
        downloaded = CLASSES_DIR / f"{class_name}.py"
        return downloaded.is_file()

    def load_from_source(self, class_name: str, source_code: str):
        """Incarcare dinamica: scrie sursa pe disk si importa modulul."""
        path = CLASSES_DIR / f"{class_name}.py"
        path.write_text(source_code, encoding="utf-8")
        return self._import_file(class_name, path)

    def load_local(self, class_name: str):
        path = Path(__file__).resolve().parent / "tasks" / f"{class_name}.py"
        if not path.is_file():
            raise FileNotFoundError(f"class not found: {class_name}")
        return self._import_file(class_name, path)

    def _import_file(self, class_name: str, path: Path):
        with self._registry_lock:
            if class_name in self._loaded:
                return self._loaded[class_name]

            spec = importlib.util.spec_from_file_location(class_name, path)
            if spec is None or spec.loader is None:
                raise ImportError(f"cannot load {class_name}")
            module = importlib.util.module_from_spec(spec)
            sys.modules[class_name] = module
            spec.loader.exec_module(module)
            self._loaded[class_name] = module
            return module

    def run_parallel(
        self,
        class_name: str,
        method_name: str,
        args_list: list,
        thread_count: int,
        on_result,
        on_done,
    ) -> None:
        """
        Porneste thread_count fire. Fiecare fir:
        - apeleaza metoda cu un argument din args_list (ciclic daca lista e scurta)
        - la final apeleaza on_result(thread_id, result)
        - la finalul tuturor firelor apeleaza on_done()
        """
        module = self._loaded.get(class_name) or self.load_local(class_name)
        if not hasattr(module, class_name):
            raise AttributeError(f"class {class_name} missing in module")
        cls = getattr(module, class_name)
        instance = cls()
        if not hasattr(instance, method_name):
            raise AttributeError(f"method {method_name} not found")

        method = getattr(instance, method_name)
        if thread_count < 1:
            raise ValueError("thread_count must be >= 1")

        if not args_list:
            args_list = [None]

        done_lock = threading.Lock()
        done_count = {"n": 0}
        errors = []

        def worker(tid: int):
            arg = args_list[tid % len(args_list)]
            try:
                result = method(arg)
                on_result(tid, {"ok": True, "result": result})
            except Exception as exc:
                on_result(tid, {"ok": False, "error": str(exc)})
                errors.append(str(exc))
            finally:
                with done_lock:
                    done_count["n"] += 1
                    if done_count["n"] >= thread_count:
                        on_done(errors)

        threads = []
        for i in range(thread_count):
            t = threading.Thread(target=worker, args=(i,), daemon=True)
            threads.append(t)
            t.start()

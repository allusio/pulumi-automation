from __future__ import annotations

from contextlib import suppress
import functools
import os
from pathlib import Path
import sys
import time

from pulumi.automation import (
    EngineEvent, LocalWorkspace, LocalWorkspaceOptions,
    ProjectBackend, ProjectSettings, Stack, StackSettings, StackSummary,
    create_or_select_stack, create_stack, select_stack, CommandError
)


def _patch_get_all_config(stack):
    """
    Workaround for https://github.com/pulumi/pulumi/issues/7282
    """
    import yaml
    from pulumi.automation._local_workspace import LocalWorkspace
    origin_get_all_config = LocalWorkspace.get_all_config

    def get_all_config(self, stack_name):
        deployment = stack.export_stack().deployment
        config_file = f"{vars(self)['work_dir']}/Pulumi.{stack_name}.yaml"
        with open(config_file) as f:
            config = yaml.safe_load(f)
            config["secretsprovider"] = deployment['secrets_providers']['type']
            if encryptionsalt := deployment['secrets_providers']['state'].get('salt'):
                config["encryptionsalt"] = encryptionsalt
            # TODO add support for encryptedkey after secret provider is chosen
        with open(config_file, "w") as f:
            yaml.safe_dump(config, f)
        return origin_get_all_config(self, stack_name)

    LocalWorkspace.get_all_config = get_all_config


class PulumiProject:
    def __init__(self, project_name, backend_url, verbose_more=None):
        self.project_name = project_name
        self.project_settings = ProjectSettings(
            name=project_name,
            runtime="python",
            backend=ProjectBackend(url=backend_url),
        )

        is_venv = hasattr(sys, 'real_prefix') or sys.base_prefix != sys.prefix
        if not is_venv:
            raise Exception("Deployment must be run inside virtualenv.")

        self.env_vars = {
            "PULUMI_CONFIG_PASSPHRASE": "",
            "PATH": f"{os.environ['PATH']}:{Path.home() / '.pulumi/bin/'}",
            "PULUMI_RUNTIME_VIRTUALENV": sys.prefix,
            "PULUMI_BACKEND_URL": backend_url,
        }

        self.verbose_more = verbose_more or (lambda x: None)

    def _prepare_stack_kwargs(
        self,
        stack_name,
        secrets_provider,
        program,
    ):
        stack_settings = StackSettings(
            secrets_provider=secrets_provider,
        )

        return {
            "project_name": self.project_name,
            "stack_name": stack_name,
            "program": program,
            "opts": LocalWorkspaceOptions(
                project_settings=self.project_settings,
                env_vars=self.env_vars,
                secrets_provider=secrets_provider,
                stack_settings={stack_name: stack_settings},
            )
        }

    def _workspace(self, work_dir: str = None):
        return LocalWorkspace(
            project_settings=self.project_settings,
            env_vars=self.env_vars,
            work_dir=work_dir,
        )

    def _augment_stack(self, stack: Stack):
        changing_resources = {}

        def on_event(event: EngineEvent):
            if resource_pre_event := event.resource_pre_event:
                metadata = resource_pre_event.metadata
                changing_resources[metadata.urn] = time.monotonic()
                resource_name = metadata.urn.rsplit('::', 1)[-1]
                self.verbose_more(
                    f"PRE {metadata.op.value} {metadata.type} {resource_name}"
                    f"\nPRE {metadata}"
                )
            elif res_outputs_event := event.res_outputs_event:
                metadata = res_outputs_event.metadata
                start_time = changing_resources.pop(metadata.urn)
                duration = time.monotonic() - start_time
                resource_name = metadata.urn.rsplit('::', 1)[-1]
                self.verbose_more(
                    f"POST {metadata.op.value} {metadata.type} {resource_name} {duration:.1f}s"
                    f"\nPOST {metadata}"
                )
            elif res_op_failed_event := event.res_op_failed_event:
                self.verbose_more(f"FAIL {res_op_failed_event.metadata}")
            elif summary_event := event.summary_event:
                for op_display, count in summary_event.resource_changes.items():
                    self.verbose_more(f"SUM {op_display}: {count}")

                self.verbose_more(
                    f"SUM Duration: {summary_event.duration_seconds:.0f}s"
                )

        stack.up = functools.partial(stack.up, on_event=on_event)

        # TODO remove after https://github.com/pulumi/pulumi/issues/7282 is fixed
        _patch_get_all_config(stack)

        def _augument_stack_refresh(stack_refresh_method):
            @functools.wraps(stack_refresh_method)
            def _stack_refresh(*args, **kwargs):
                # TODO it might be dangerous to hide an error
                with suppress(CommandError):
                    return stack_refresh_method(*args, **kwargs)

            return _stack_refresh

        stack.refresh = _augument_stack_refresh(stack.refresh)

        return stack

    def list_stacks(self) -> list[StackSummary]:
        return self._workspace().list_stacks()

    def create_stack(self, stack_name, secrets_provider, program) -> Stack:
        return self._augment_stack(create_stack(
            **self._prepare_stack_kwargs(
                stack_name=stack_name,
                secrets_provider=secrets_provider,
                program=program,
            )
        ))

    def select_stack(self, stack_name, secrets_provider, program=None) -> Stack:
        return self._augment_stack(select_stack(
            **self._prepare_stack_kwargs(
                stack_name=stack_name,
                secrets_provider=secrets_provider,
                program=program or (lambda: None),
            )
        ))

    def create_or_select_stack(
        self,
        stack_name,
        secrets_provider,
        program=None
    ) -> Stack:
        return self._augment_stack(create_or_select_stack(
            **self._prepare_stack_kwargs(
                stack_name=stack_name,
                secrets_provider=secrets_provider,
                program=program or (lambda: None),
            )
        ))

    def remove_stack(self, stack_name):
        self._workspace().remove_stack(stack_name)

    def save_project_settings(self, work_dir: str):
        self._workspace(work_dir).save_project_settings(self.project_settings)

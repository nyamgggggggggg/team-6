"""Serving layer — policy-based consumer access control.

Import individual submodules directly to avoid circular-import chains:

    from team_data_platform.serving_layer.adapter_registry import ConsumerAdapterRegistry
    from team_data_platform.serving_layer.policy import AccessPolicyService
    from team_data_platform.serving_layer.data_serving import DataServingService
"""
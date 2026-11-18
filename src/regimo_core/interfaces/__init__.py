"""Kafka message header / metadata definitions required for ontology awareness.

Every message placed on the bus carries a standardised set of headers so that
downstream sockets can meaningfully interpret, trace and lineage-check the
payload. See :mod:`regimo_core.interfaces.message` for the concrete contract.
"""

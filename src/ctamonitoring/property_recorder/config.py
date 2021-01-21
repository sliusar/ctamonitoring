"""
Contains the configuration holder for the property recorder

The module with contains all what is related to the configuration holding
for the property recorder frontend

@author: igoroya
@organization: DESY Zeuthen
@copyright: cta-observatory.org
@version: $Id$
@change: $LastChangedDate$
@change: $LastChangedBy$
@requires: enum
@requires: ACS
@requires: ctamonitoring.property_recorder.backend
@requires: ctamonitoring.property_recorder.constants
@requires: ctamonitoring.property_recorder.util
@requires: Acspy.Common.Log
@requires: Acspy.Common
@requires: Acspy.Util
"""
from enum import Enum
from ACS import NoSuchCharacteristic  # @UnresolvedImport
from ctamonitoring.property_recorder.constants import PROPERTY_ATTRIBUTES
from ctamonitoring.property_recorder.util import attribute_decoder
from ctamonitoring.property_recorder.backend import get_registry_class

__version__ = "$Id$"


BACKEND_TYPE = Enum('BACKEND_TYPE', 'DUMMY LOG MYSQL MONGODB')


def get_registry(reg_name):
    if reg_name is BACKEND_TYPE.DUMMY:
        return get_registry_class("dummy")
    elif reg_name is BACKEND_TYPE.LOG:
        return get_registry_class("log")
    elif reg_name is BACKEND_TYPE.MYSQL:
        return None
    elif reg_name is BACKEND_TYPE.MONGODB:
        return get_registry_class("mongodb")
    else:
        raise KeyError


class RecorderConfig(object):
    """
    Holds the configuration from the property recorder

    @ivar default_timer_trigger:  Monitoring rate for those properties
    with no CDB entry for the monitoring period in seconds (default 60 s)
    @type default_timer_trigger: float
    @ivar max_comps: Maximum number of components accepted by this
    property recorder (default 100)
    @type max_comps: long
    @ivar max_props: Maximum number of properties being monitored
    (default 1000)
    @type max_props: long
    @ivar checking_period: Period in seconds to check for lost components
    or new components (default 10 s)
    @ivar backend_type: The backend to be used in the recorder
    (Default DUMMY)
    @type backend_type: ctamonitoring.property_recorder.BACKEND_TYPE
    @ivar backend_config: Configuration parameters for the backend
    (Default None)
    @type backend_config: dict
    @ivar is_include_mode:  If True, the recorder will only consider the
    components included in the list components and reject all the others
    (a include list). If set to False, will consider all the components
    except those in the list (A exclude list)
    @type is_include_mode: boolean
    @ivar components: The include or exclude list, depending on the value of
    is_include_mode, of component represented by their string names
    @type components: set
    """

    def __init__(self):
        """
        Initializes the values to those defined as default

        The default values are:
        default_timer_trigger = 60.0 seconds
        max_comps = 100
        max_props = 1000
        checking_period (for new components) = 10 seconds
        backend_type = BACKEND_TYPE.DUMMY
        backend_config = None
        is_include_mode = False
        """
        # 1/min, units in in 100 of ns, OMG time
        self._default_timer_trigger = 60.0
        # will not accept more components if this number is exceeded
        self._max_comps = 100
        # will not accept more components if the total number of props is this
        # number or more
        self._max_props = 1000
        self._checking_period = 10  # seconds
        self._backend_type = BACKEND_TYPE.DUMMY
        self.backend_config = None

        self._is_include_mode = False

        self._components = set()

    @property
    def default_timer_trigger(self):
        """"
        The monitoring rate in s to be used when no input is provided
        in the CDB

        @raise ValueError: When input type is incorrect or value is negative
        """
        return self._default_timer_trigger

    @default_timer_trigger.setter
    def default_timer_trigger(self, default_timer_trigger):
        rate = float(default_timer_trigger)
        if rate < 0.0:
            raise ValueError("default_monitoring_rate type must be positive")
        self._default_timer_trigger = rate

    @property
    def max_comps(self):
        """"
        The maximum number of components that the recorder will monitor.

        @raise ValueError: When input type is incorrect or value is negative
        """
        return self._max_comps

    @max_comps.setter
    def max_comps(self, max_comps):
        comps = int(max_comps)
        if comps < 1:
            raise ValueError("max_comps type must be positive")
        self._max_comps = comps

    @property
    def max_props(self):
        """"
        The total maximum number of properties, including all the components,
        that the recorder will monitor.

        @raise ValueError: When input type is incorrect or value is negative
        """
        return self._max_props

    @max_props.setter
    def max_props(self, max_props):
        props = int(max_props)
        if props < 1:
            raise ValueError("max_props type must be positive")
        self._max_props = props

    @property
    def checking_period(self):
        """"
        The period in s that the recorder uses to find new
        components in the system

        @raise ValueError: When input type is incorrect or value is negative
        """
        return self._checking_period

    @checking_period.setter
    def checking_period(self, checking_period):
        period = int(checking_period)
        if period < 1:
            raise ValueError("checking_period checking period must be > 1 s")
        self._checking_period = period

    @property
    def backend_type(self):
        return self._backend_type

    @backend_type.setter
    def backend_type(self, backend_type):
        if backend_type not in BACKEND_TYPE:
            raise ValueError(
                "Backend type not recognized. Supported types are " +
                str([e.name for e in BACKEND_TYPE]))
        self._backend_type = backend_type

    @property
    def is_include_mode(self):
        """"
        The mode to handle the component list. If true, the property recorder
        works in include mode which means that will only monitor those
        components in the list

        If false, the list will be used as an exclude mode, and therefore
        any component accessible will be used except those in the list
        """
        return self._is_include_mode

    @is_include_mode.setter
    def is_include_mode(self, include_mode):
        if not isinstance(include_mode, bool):
            raise TypeError("include_mode must be True or False")
        self._is_include_mode = include_mode

    @property
    def components(self):
        return self._components

    @components.setter
    def components(self, components):
        raise NotImplementedError(
            "Cannot mutate, components are set by setComponentList")

    def set_components(self, components):
        """
        Replaces the actual list of components by the provided one.

        @raise ValueError: If any of the components in the list is not str
        """
        if not isinstance(components, set):
            raise TypeError("A set of str needs to be provided")

        for component in components:
            if not isinstance(component, str):
                raise TypeError(
                    "components need to be represented as str, a " +
                    str(type(component)) + "was provided")

        self._components = components


def get_prop_attribs_cdb(acs_property):
    """
    Gets attributes from a property and creates a map with
    attribute name, value

    @param acs_property: the ACS property object
    @return: dictionary of attribute types and values
    @rtype: dictionary
    """

    attributes = {}
    for attribute in PROPERTY_ATTRIBUTES:
        attributes[attribute.name] = (
            get_cdb_entry(attribute, acs_property)
            )

    attributes['name'] = acs_property._get_name()

    return attributes


def get_prop_attribs_cdb_xml(acs_property):
    """
    Gets attributes from a property from an objectified XML

    Gets attributes from a property using the XML objectifier
    by creating a map of attribute name, value.

    This needs to be used for python characteristic components,
    as the property attributes are empty for those components.

    @see:
    ctamonitoring.property_recorder.util.ComponentUtil.get_objectified_cdb

    @param acs_property: the ACS property from the objectified XML
    @return: dictionary of attribute types and values
    @rtype: dictionary
    """

    attributes = {}
    for attribute in PROPERTY_ATTRIBUTES:
        attributes[attribute.name] = (
            get_cdb_entry_xml(attribute, acs_property)
        )

    attributes['name'] = acs_property.nodeName

    return attributes


def get_cdb_entry(attribute, acs_property):
    """
    Decode a cdb attribute entry for a property

    Stardard method, used with Java and C++ components
    @param attribute: attribute name to get
    @type attribute: string
    @param acs_property: the ACS property
    @return: decoded cdb entry
    """

    try:
        if len(acs_property.find_characteristic(
                attribute.name)) is 0:
            return None
        raw_value = acs_property.get_characteristic_by_name(
            attribute.name).value()
    except NoSuchCharacteristic:
        return None

    return process_attribute(attribute, raw_value)


def get_cdb_entry_xml(attribute, acs_property_cdb):
    """
    Decode a cdb attribute entry for a property

    Used with Python components Would also work with C++ and Java
    components but the performance is much worse

    @param attribute: attribute name to get
    @type attribute: string
    @param acs_property_cdb: the ACS property objectified CDB
    @type acs_property_cdb: xml.dom.minidom.Element
    @return: decoded cdb entry
    """
    raw_value = acs_property_cdb.getAttribute(attribute.name)
    return process_attribute(attribute, raw_value)


def process_attribute(attribute, raw_value):
    """
    Parses and returns a CDB attribute

    @param attribute: attribute to process
    @type attribute: string
    @param raw_value: raw value from the CDB
    @type acs_property_cdb: xml.dom.minidom.Element
    @return: decoded/parsed attribute
    """
    try:
        value = attribute_decoder.decode_attribute(
            raw_value, attribute.decoding)
    except ValueError:
        # If is boolean and the decoding fails we try to catch it here
        try:
            value = attribute_decoder.decode_boolean(raw_value)
        except (ValueError, TypeError):
            value = None

    # Check those cases when it has to be positive
    if (attribute.isPositive) and (value is not None) and (value < 0.0):
        value = None

    # Check those cases when it has to contain specific keyword synonym to
    # yes, true etc
    if (value is not None) and (attribute.yes_synonyms is not None):
        value_lower = value.lower()
        found = False
        for entry in attribute.yes_synonyms:
            if value_lower == entry:
                found = True

        value = found

    return value

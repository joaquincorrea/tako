"""

`tigres.core.graph`
*******************

.. currentmodule:: tigres.core.graph

:platform: Unix, Mac
:synopsis: The core Tigres graph


Module for handling .dot file generation from current state of execution graph.
Generation of execution graphs.


Classes
=========
 * :class:`DirectedGraph` - A representation of a directed graph
 * :class:`DotFormat` -
 * :class:`GraphError` - Exception class for graph errors


Functions
=========
 * :func:`dot_execution` - create a DOT execution graph for the currently running Tigres Program


.. moduleauthor:: Gilberto Pastorello <gzpastorello@lbl.gov>, Ryan Rodriguez <ryanrodriguez@lbl.gov>


"""
from copy import copy

from tigres.core.monitoring import Program
from tigres.core.state.work import WorkSequence, WorkParallel, WorkUnit, \
    CURRENT_INDEX


try:
    from itertools import tee, zip_longest
except ImportError:
    from itertools import tee, izip_longest

    zip_longest = izip_longest
from tigres.core.utils import TigresInternalException

from tigres.utils import State, TigresException


def dot_execution(path="."):
    """
    Creates an execution graph of the currently running Tigres program
    and writes it to a file

    :param path: the file path to write the DOT file to
    :return:
    :sideeffect: writes a DOT file of the currently running Tigres :class:`tigres.core.monitoring.state.Program`
    """
    program = Program()
    filename = "{}/{}.dot".format(path, _friendly_name(program))

    dot_string = dot_execution_string(program.root_work)

    with open(filename, 'w') as f:
        f.writelines(dot_string)


def dot_execution_string(root_work):
    templates = ''
    edges = ''
    task_nodes = ''

    templates, edges, task_nodes = _dot_sequence(0, templates, edges,
                                                 task_nodes, root_work)

    return DotFormat.OUT_GRAPH.format(
        workflow_name='"{}"'.format(root_work.name),
        digraph_attrs=_f_attrs(DotFormat.DIGRAPH),
        subgraphs=templates,
        edges=edges)

class GraphError(Exception):
    pass


class DirectedGraph(object):
    """
    General Directed Graph - Adjacency list implementation

    This graph only allows a child to have one parent


    """

    def __init__(self):
        self._vertices = {}
        self._roots = []

    def add_vertex(self, vertex):
        """

        :param vertex: vertex to add to the graph
        :return: None
        """

        # Check for vertex uniqueness
        vertex_exists = self.has_vertex(vertex)
        if not vertex_exists:
            self._vertices[vertex] = None

        if vertex_exists:
            raise GraphError("Node must be unique: %s" % vertex)

    def add_edge(self, parent, child):
        """

        Add an edge to the vertex.

        :param parent: parent vertex
        :param child: child vertex
        :return: None
        :exception GraphError: if edge already exists or if vertices do not exist
        """

        if not child or not parent:
            raise GraphError("Edge must have two vertices - parent: (%s,%s)" % (
                parent, child))

        # Check to see if vertices exist
        vertices_exist = self.has_vertices([parent, child])

        # Check for edge uniqueness
        edge_exists = self.has_edge(parent, child)

        # Add the vertex if the edge is not there
        if not edge_exists and vertices_exist:
            if self.is_leaf(parent):
                self._vertices[parent] = []
            self._vertices[parent].append(child)

        if edge_exists:
            raise GraphError("Edge must be unique: %s" % str((parent, child)))
        if not vertices_exist:
            raise GraphError(
                "All vertices in the edge must exist in the graph: (%s,%s)" % (
                    parent, child))

    def has_vertex(self, vertex):

        return vertex in self._vertices

    def has_vertices(self, vertices):

        return len(set(vertices) & set(self._vertices.keys())) == len(vertices)

    def is_leaf(self, vertex):
        if not self.has_vertex(vertex):
            raise GraphError("Node is doesn't exist: %s" % vertex)
        return self._vertices[vertex] is None

    def has_edge(self, parent, child):

        if not self.has_vertices([parent, child]):
            raise GraphError(
                "Not all vertices exist: %s" % str((parent, child)))

        return child in self._vertices[parent]

    @property
    def vertices(self):
        return list(self._vertices.keys())

    def children(self, vertex):
        return self._vertices[vertex]

    def leaves(self, vertex):
        leaves = []
        for c in self.children(vertex):
            if self.is_leaf(c):
                leaves.append(c)
        return leaves

    def __contains__(self, n):
        return n in self.vertices


def _f_attrs(attrs):
    return ' '.join(
        [str(key) + '=' + str(value) for key, value in attrs.items()])


class DotFormat(object):
    """Attributes for formatting dot files
    """
    # Use Gilberto's model of using a DotFormat class to move formatting outside of the 'generate' method
    DIGRAPH = {"rankdir": "LR", "bgcolor": "white",
               "compound": "true" + " node[fontColor=white fontname=halvetica]"}
    DIGRAPH_DEP = {"rankdir": "LR", "bgcolor": "white", "compound": "true",
                   "ranksep": "1",
                   "nodesep": "1" + " node[fontColor=white fontname=halvetica]"}
    TASK_NEW = {"shape": "box", "style": "filled", "fillcolor": "cyan"}
    TASK_DONE = {"shape": "ellipse", "style": "filled",
                 "fillcolor": "royalblue4", "fontcolor": "white"}
    TASK_FAIL = {"shape": "diamond", "style": "filled", "fillcolor": "red"}
    #invisible edge fo formatting
    NODE = {"shape": "ellipse", "style": "filled", "fillcolor": "royalblue4",
            "fontcolor": "white"}
    DEPENDENT_EDGE = {"color": "red", "concentrate": "true"}
    TEMPLATE_EDGE = {"color": "black", "style": "dotted"}
    TASK_EDGE = {"color": "black"}
    OUT_DEPENDENT_EDGE_TEMPLATE = "{id1}->{id2}[{attrs} + ltail=cluster_{id3} lhead = cluster_{id4}"
    OUT_EXECUTION_EDGE = "[ltail=cluster_{id2} lhead = cluster_{id2} color = black, style = dashed]"
    OUT_GRAPH = "digraph {workflow_name}\n{{\n{digraph_attrs}\n\n{subgraphs}\n{edges}}}"
    OUT_SUBGRAPH = "subgraph {id} {{{label} {rank} {fontColor} {color}\n{nodes}\n}}\n"
    OUT_NODE = "{id} [{attrs}]\n"
    OUT_EDGE = "{id1} -> {id2} [{attrs}]\n"

    """Attributes for formatting JSON files
    """

    OUT_DEPENDENCY = " "'"name"'": "'"{id1}"'", "'"template"'":"'"{id2}"'" ,  "'"dependencies"'": [ {id3} ] "
    OUT_JSONEDGE = " "'"source"'":"'"{id1}"'", "'"target"'":"'"{id2}"'", "'"visible"'":"'"{id3}"'" "
    OUT_JSON = "[{id1}]"

    @classmethod
    def task_style_by_state(cls, state):
        style = {
            State.BLOCKED: cls.TASK_NEW,
            State.DONE: cls.TASK_DONE,
            State.FAIL: cls.TASK_FAIL,
            State.NEW: cls.TASK_NEW,
            State.RUN: cls.TASK_NEW,
            State.UNKNOWN: cls.TASK_FAIL,
        }.get(state, None)
        if style is None:
            raise TigresInternalException(
                "Unknown state for graph formatting '{}'".format(state))
        return style


def _dot_parallel(work_parallel, task_nodes):
    """

    :param work_parallel: parallel state to create DOT nodes for
    :type work_parallel: WorkParallel
    :param task_nodes:
    :return:
    """
    if not isinstance(work_parallel, WorkParallel):
        raise TigresException(
            "Error writing DOT file expecting a WorkParallel object not {}".format(
                work_parallel.type))
    for work in work_parallel:
        task_nodes = _dot_work(work, task_nodes)
    return task_nodes


def _dot_work(work, task_nodes):
    """

    :param work: The leave not to write to the DOT file
    :type work: WorkUnit
    :param task_nodes: string of nodes for writing to the DOT file
    :return: modified string of nodes for writing to the DOT file
    """

    node_attrs = DotFormat.task_style_by_state(work.state).copy()
    node_attrs.update({'label': '"{}"'.format(work.name)})
    task_nodes += DotFormat.OUT_NODE.format(id=_friendly_name(work),
                                            attrs=_f_attrs(node_attrs))
    return task_nodes


def _dot_template_edge(template_id, current_ids, edge_attrs, previous_ids,
                       previous_work):
    """
    Add a template edge is an edge that comes from a template instead of individual
    state.

    :param template_id: The unique identifier of the template to add the edge for
    :param current_ids: the "to" vertices
    :param edge_attrs: the edge attributes to use
    :param previous_ids: the "from" vertices
    :param previous_work: the previous state to build the edge form
    :return:
    """
    # template edge
    if isinstance(previous_work, WorkUnit):
        previous_ids.append(_friendly_name(previous_work))
    elif isinstance(previous_work, WorkSequence):
        previous_ids.append(_friendly_name(previous_work[CURRENT_INDEX]))
    elif isinstance(previous_work, WorkParallel):
        previous_ids.append(
            _friendly_name(previous_work[int(len(previous_work) / 2)]))
    edge_attrs['ltail'] = "cluster_{}".format(template_id - 1)
    edge_attrs['lhead'] = "cluster_{}".format(template_id)
    if len(current_ids) > 1:
        current_ids = [current_ids[int(len(current_ids) / 2)]]
    return current_ids


def _dot_edges(edges, work, template_id):
    """
    Generate the DOT incoming edges for the specified state

    :param edges: string representing DOT edges
    :param work: the state nodes to write incoming DOT edges for
    :type work: WorkBase
    :return: modified string representing DOT edges
    """
    edge_attrs = copy(DotFormat.TASK_EDGE)
    if work.previous:
        # There is previous state, so we create add an edge.
        previous_ids = []
        current_ids = []
        previous_work = work.previous

        # determine the current state ids (where the edges are going to)
        if isinstance(work, WorkParallel):
            # The current state ids for the parallel is all the state in the list
            for workp in work:
                current_ids.append(_friendly_name(workp))
        elif isinstance(work, WorkUnit):
            # There is only one state id for an individual state unit
            current_ids.append(_friendly_name(work))
        elif isinstance(work, WorkSequence):
            # The current state id for a sequences is the last item
            current_ids.append(_friendly_name(work[CURRENT_INDEX]))

        # Now, we determine the previous state id (where the edges are coming from)
        work_parent = work.parent
        previous_parent = previous_work.parent

        if isinstance(previous_work, WorkParallel):
            # We need to determine if the edge is between
            # a WorkUnit or another template.
            if isinstance(work, WorkUnit):
                #If the previous work is a work unit then we want
                # the edges from each task to point to the WorkUnit
                for workp in previous_work:
                    previous_ids.append(_friendly_name(workp))
            else:
                # The current and previous work are both templates.
                # so their edges are  between the previous template
                # and the current template.
                current_ids = _dot_template_edge(template_id, current_ids,
                                                 edge_attrs, previous_ids,
                                                 previous_work)
        elif isinstance(previous_work, WorkUnit):
            # If the previous state is an individual unit,
            # we need to determine if the current state and it
            # have the same parent or not.
            if work_parent == previous_parent:
                # They have the same parent, so their edges are
                # directly between one another
                previous_ids.append(_friendly_name(previous_work))
            else:
                # The current and previous state do not have the same parent.
                # so their edges are different. it is between the previous template
                # and the current state.
                current_ids = _dot_template_edge(template_id, current_ids,
                                                 edge_attrs, previous_ids,
                                                 previous_work)

        elif isinstance(previous_work, WorkSequence):
            # The previous state is a sequence so we need to grab the last state item finished
            pwork = previous_work[CURRENT_INDEX]
            if pwork.parent != work.parent:
                current_ids = _dot_template_edge(template_id, current_ids,
                                                 edge_attrs, previous_ids,
                                                 pwork)

        # OK now we create the edges from the previous and current ids
        for p in previous_ids:
            for c in current_ids:
                edges += DotFormat.OUT_EDGE.format(id1=p, id2=c,
                                                   attrs=_f_attrs(edge_attrs))

    return edges


def _dot_sequence(template_id, templates, edges, task_nodes, root_work):
    """
    Generate the DOT string for a WorkSequence

    :param edges: string representing DOT edges
    :param task_nodes: string of nodes for writing to the DOT file
    :param state: the state node write a DOT file for
    :type: WorkBase
    :return: tuple of edges and nodes
    """

    for work in root_work:
        if isinstance(work, WorkUnit):
            task_nodes = _dot_work(work, task_nodes)
            edges = _dot_edges(edges, work, template_id)
        elif isinstance(work, WorkParallel):
            task_nodes = _dot_parallel(work, task_nodes)
            edges = _dot_edges(edges, work, template_id)
        elif isinstance(work, WorkSequence):
            templates, edges, task_nodes = _dot_sequence(template_id, templates,
                                                         edges, task_nodes,
                                                         work)

        # If the parent of the current state does not have a parent we are at a template level
        if not work.parent.parent:
            templates += DotFormat.OUT_SUBGRAPH.format(
                id="cluster_{}".format(template_id), rank='', nodes=task_nodes,
                label='label=' + '"' + work.name + '"',
                fontColor="fontcolor=black;", color="color=black;")
            template_id += 1
            task_nodes = ''

    return templates, edges, task_nodes


def _friendly_name(work):
    """
    Create a user friendly name for the given object
    :param work:
    :return:
    """
    if work and hasattr(work, "name"):
        return "".join(x for x in work.name if x.isalnum() or x in '_')
    else:
        return None






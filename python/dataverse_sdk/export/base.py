import abc
from collections.abc import Generator
from typing import Callable


class ExportAnnotationBase(abc.ABC):
    @abc.abstractmethod
    def producer(
        self,
        target_folder: str,
        class_names: list[str],
        sequence_frame_map: dict[int, dict[int, list[int]]],
        datarow_generator_func: Callable[[list], Generator[dict]],
        annotation_name: str,
        is_sequential: bool,
        *args,
        **kwargs,
    ) -> Generator[bytes, str]:
        """
        Parameters
        ----------
        target_folder : str
            _description_
        class_names : list[str]
            list of class name
        sequence_frame_map : dict[int, dict[int, list[int]]]
            the entire tree of sequence, frame and base datarows
        datarow_generator_func : Callable[[list], AsyncGenerator[dict]]
            a function that takes list of id as input which generates corresponding datarows
        annotation_name: str
            an annotation name

        Returns
        -------
        AsyncGenerator[bytes, str]
            should yield a tuple with the data to upload and the relative path to rootfolder
            relative path: this/is/the/relative/path/to/file.txt
        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError
